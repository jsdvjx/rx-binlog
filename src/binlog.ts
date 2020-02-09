import { spawn, execSync } from "child_process"
import { Observable, Observer, of, zip, Subject, interval, from } from 'rxjs'
import * as mysql from 'mysql2'
import { MysqlSchema, MysqlColumn, MysqlMap } from "./mysql.schema";
import { tap, map, switchMap, toArray, mergeMap } from "rxjs/operators";
import { Query } from "./query";
import { Conn } from './conn'
import moment, { unix } from "moment";
import { writeFileSync, readFileSync, existsSync, mkdirSync, statSync, readdirSync, writeFile } from "fs";
import { BinlogResolver, EVENT_TYPE, EventHead } from "./binlog.resolver";
import os from 'os'
import { Logs } from "./logs";
export type SqlType = "UPDATE" | "INSERT" | "DELETE"
export interface BinlogEvent extends EventHead {
    qtype: SqlType,
    database: string,
    table: string,
    pk?: string,
    uni?: string[],
    emit_at: Date,
    value?: string | number
    source: Record<string, any>
    update?: Record<string, any>
    file: string
}

export interface BinlogOption {
    host: string
    port: number
    username: string
    password: string
    database?: string
}
export type optionName = 'base64-output' | 'bind-address' | 'binlog-row-event-max-size' | 'character-sets-dir' | 'connection-server-id' | 'database' | 'debug' | 'debug-check' | 'debug-info' | 'default-auth' | 'defaults-extra-file' | 'defaults-file' | 'defaults-group-suffix' | 'disable-log-bin' | 'exclude-gtids' | 'force-if-open' | 'force-read' | 'help' | 'hexdump' | 'host' | 'include-gtids' | 'local-load' | 'login-path' | 'no-defaults' | 'offset' | 'open_files_limit' | 'password' | 'plugin-dir' | 'port' | 'print-defaults' | 'protocol' | 'raw' | 'read-from-remote-master' | 'read-from-remote-server' | 'result-file' | 'secure-auth' | 'server-id' | 'server-id-bits' | 'set-charset' | 'shared-memory-base-name' | 'short-form' | 'skip-gtids' | 'socket' | 'ssl-crl' | 'ssl-crlpath' | 'ssl-mode' | 'start-datetime' | 'start-position' | 'stop-datetime' | 'stop-never' | 'stop-never-slave-server-id' | 'stop-position' | 'to-last-log' | 'user' | 'verbose' | 'verify-binlog-checksum' | 'version'
export type option = {
    [k in optionName]?: string | number;
};
export interface MysqlBinlogFile { Log_name: string, File_size: number }
export class Binlog {
    private path = `${__dirname}/../bin/mysqlbinlog`
    private localLog = new Logs
    constructor(private option: BinlogOption) {
        if (os.platform() === 'linux') {
            execSync(`chmod +x ${this.path}`)
        }
    }
    private conn = Conn.create(this.option)
    private ms = new MysqlSchema(this.conn)
    private query = new Query(this.conn)
    private _schema!: MysqlMap;
    private pos: number = 0;
    private file: string = ''
    private offset: number = 0;
    private get ready() {
        return this._schema ? of(this._schema) : this.ms.schema().pipe(tap(schema => this._schema = schema))
    }
    private setParameter: option = {}
    setOption = (option: option) => {
        this.setParameter = { ...this.setParameter, ...option }
        return this;
    }
    get parameter() {
        return Object.entries(this.setOption).map(([key, value]) => {
            return `--${key}=${typeof value === 'string' ? `'${value}'` : value}`
        })
    }
    private head2event = (head: EventHead): BinlogEvent | null => {
        if (![EVENT_TYPE.UPDATE_ROW_EVENT, EVENT_TYPE.WRITE_ROW_EVENT, EVENT_TYPE.DELETE_ROW_EVENT].includes(head.type)) {
            return null
        }
        const table_id = head.ext.table_id;
        const db = this.dbMap[table_id];
        return { ...head, ...db, ...(this.getDbInfo(db) || {}), source: {}, qtype: head.qtype as SqlType }
    }
    private getDbInfo = (db: { database: string, table: string }): { pk: string, uni: (string)[] } | null => {
        const map = db ? (this._schema[db.database] ? this._schema[db.database][db.table] : null) : null;
        if (map === null) return null;
        const cols = Object.values(map)
        const set = new Set<string>();
        const result: { pk: string, uni: string[] } = { pk: '', uni: [] }
        for (const col of cols) {
            if (col.COLUMN_KEY === 'PRI') {
                result.pk = col.COLUMN_NAME;
            }
            if (col.COLUMN_KEY === 'UNI') {
                set.add(col.COLUMN_NAME)
            }
        }
        result.uni = Array.from(set)
        return result;
    }
    private static transform = (schema: MysqlMap, event: BinlogEvent) => {
        const map = schema[event.database] ? schema[event.database][event.table] : null;
        if (!map) return event;
        const cols = Object.values(map)
        for (const col of cols) {
            if (col.COLUMN_KEY === 'PRI') {
                event.pk = col.COLUMN_NAME;
            }
            if (col.COLUMN_KEY === 'UNI') {
                event?.uni?.push(col.COLUMN_NAME)
            }
        }
        const resolve = (row: Record<string, string>) => {
            return Object.entries(row).reduce((res, [key, _value]) => {
                if (!map[key]) return res;
                const info = map[key] as MysqlColumn;
                const name = info.COLUMN_NAME;
                if (/^'[\w\W]*?'$/) {
                    _value = /^'(?<content>[\w\W]*?)'$/.exec(_value)?.groups?.content || _value
                }

                res[name] = _value === `''` ? '' : _value;
                if (info.IS_NULLABLE === 'YES' && _value == 'NULL') {
                    res[name] = null;
                } else switch (info.DATA_TYPE) {
                    case 'bigint':
                    case 'char':
                    case 'double':
                    case 'decimal':
                    case 'float':
                    case 'int':
                    case 'tinyint':
                    case 'smallint':
                    case 'mediumint':
                        if (/^\d+$/.test(_value)) {
                            res[name] = parseInt(_value);
                        } else if (/^\d+\.\d+$/.test(_value)) {
                            res[name] = parseFloat(_value);
                        }
                        break;
                    case 'longtext':
                    case 'varchar':
                    case 'text':
                    case 'mediumtext':
                        break;
                    case 'date':
                    case 'datetime':
                    case 'time':
                    case 'timestamp':
                        res[name] = (_value && _value.length > 2) ?
                            new Date(_value) : new Date(0)
                        if (_value === null) res[name] = new Date(0)
                        if (isNaN((res[name] as Date).getTime())) {
                            res[name] = new Date();
                        }
                        break;
                }

                return (res);
            }, {} as Record<string, Date | number | string | null>)
        }
        switch (event.qtype) {
            case 'UPDATE':
                event.source = resolve(event.source)
                event.update = resolve(event.update as Record<string, string>)
                break;
            case 'INSERT':
            case 'DELETE':
                event.source = resolve(event.source)
                break;
        }
        if (event?.update && event.update[event?.pk || -1]) {
            event.value = event.update[event?.pk || -1] || 0;
        } else {
            event.value = event.source[event?.pk || -1] || 0;
        }
        return event;
    }
    private static resolve = (input: string, head: BinlogEvent): BinlogEvent => {
        const reg = /@[\d]{1,2}\=.+/g
        const [source, update] = (head.qtype === 'UPDATE' ? input.split('### SET').map(i => i.match(reg)) : [input.match(reg)]) || [];
        const toRecord = (values: string[]) => {
            return values.length === 0 ? {} : values.reduce((res, acc) => (res[acc.split("=")[0]] = acc.split("=")[1], res), {} as Record<string, string>)
        }
        const result = {
            ...head, source: toRecord(source as string[]), update: toRecord((update || []))
        }
        if (head.qtype === 'UPDATE') {
            Object.keys(result.update).map(k => {
                if (result.source[k] !== undefined && result.update[k] === result.source[k]) {
                    delete result.update[k]
                }
            })
        }
        return result;
    }
    private baseParam = ['-R', '--stop-never', '-v']
    private get dbParam() {
        return [
            `--host=${this.option.host}`,
            `--password=${this.option.password}`,
            `--user=${this.option.username}`,
            ...(this.option.database ? [`--database=${this.option.database}`] : [])
        ]
    }
    private dbMap: Record<number, { database: string, table: string }> = {};
    private fixDb = (ev: BinlogEvent, str: string) => {
        const match = /`(?<database>[\w]+?)`\.`(?<table>[\w]+?)`/.exec(str);
        if (match) {
            const dbinfo = this.getDbInfo(match.groups as any)
            ev.database = match.groups?.database || '';
            ev.table = match.groups?.table || ''
            ev.pk = dbinfo?.pk
            ev.uni = dbinfo?.uni
        }
    }
    private logPath = process.cwd() + '/logs';
    read = () => {
        return this.ready.pipe(
            switchMap(() => (new BinlogResolver).stream()),
            mergeMap(({ head, body }) => {
                if (head.type === EVENT_TYPE.ROTATE_EVENT) {
                    this.file = head.file
                }
                if (head.type === EVENT_TYPE.TABLE_MAP_EVENT) {
                    const { map_to, table } = head.ext
                    const [database, _table] = table.replace(/`/g, '').split('.')
                    this.dbMap[map_to] = { database, table: _table }
                }
                const ev = this.head2event(head);
                if (ev) {
                    if (!ev.database) {
                        this.fixDb(ev, body[0] || '')
                    }
                    return from(body.map(str => Binlog.transform(this._schema, Binlog.resolve(str, ev))))
                }
                return from([])
            })
        )
    }
    run = (options: string[]) => {
        interval(1000 * 3).subscribe(
            () => {
                if (this.pos > 0 && this.file.length > 0) {
                    this.savePosition()
                }
            }
        )
        const stream = spawn(this.path, [
            ...this.baseParam,
            ...this.dbParam,
            ...options
        ]);
        return this.ready.pipe(switchMap(() => new Observable((obser: Observer<BinlogEvent>) => {
            // const br = new BinlogResolver;
            stream.stdout.on("data", (chunk: Uint8Array) => {
                this.localLog.write(chunk)
                //               br.push(chunk.toString());
            })
            // br.stream().subscribe((response) => {
            //     const [str, head] = response
            //     this.pos = head.end_log_pos
            //     if (head.type === EVENT_TYPE.ROTATE_EVENT) {
            //         this.file = head.file
            //     }
            //     if (head.type === EVENT_TYPE.TABLE_MAP_EVENT) {
            //         const { map_to, table } = head.ext
            //         const [database, _table] = table.replace(/`/g, '').split('.')
            //         this.dbMap[map_to] = { database, table: _table }
            //     }
            //     const ev = this.head2event(head);
            //     if (ev) {
            //         if (!ev.database) {
            //             this.fixDb(ev, str)
            //         }
            //         obser.next(Binlog.transform(this._schema, Binlog.resolve(str, ev)))
            //     }

            // }, (e) => { obser.error(e) }, () => obser.complete())
            stream.on('error', (err) => {
                obser.error(err);
                obser.complete();
            })
            stream.on('close', (code, sign) => {
                obser.complete();
            })
        })))
    }
    positionPath = process.cwd() + "/position.json"
    private savePosition = () => writeFileSync(this.positionPath, JSON.stringify({ file: this.file, position: this.pos }))
    autoLog = () => {
        return this.position().pipe(switchMap(this.run))
    }
    startWithLastFile = () => {
        return this.position(false).pipe(
            map(list => {
                return [...list]
            }),
            switchMap(this.run)
        )
    }
    readFrom = ({ file, position }: { file: string, position: number }) => {
        return this.run([`--start_position=${position}`, file])
    }
    private getPosition = () => existsSync(this.positionPath) ? of(JSON.parse(readFileSync(this.positionPath).toString()) as { position: number; file: string }).pipe(map(item => ({ ...item, local: true }))) : this.query.run<MysqlBinlogFile>('show BINARY logs').pipe(
        map(log => (log.Log_name = log.Log_name.replace('mysql-bin.', ''), log)),
        toArray(),
        map(logs => logs.reduce((res, acc) =>
            (res.len = acc.Log_name.length, res.idx = res.idx > parseInt(acc.Log_name)
                ? res.idx : parseInt(acc.Log_name), res.size = acc.File_size, res)
            , { idx: 0, size: 0, len: 0 } as { idx: number; size: number, len: number })
        ),
        map(tmp => {
            const file = `mysql-bin.${Array(tmp.len - tmp.idx.toString().length + 1).join('0')}${tmp.idx}`;
            return { file, position: tmp.size, local: false }
        })
    );
    private position = (pos: boolean = true) => {
        return this.getPosition().pipe(
            map(item => {
                const { file, position, local } = item;
                if (local && pos) {
                    return [
                        `--start-position=${position}`,
                        file
                    ]
                }
                return [file]
            })
        )
    }

    pgsql_ddl = this.ms.pgsql_ddl.bind(this.ms.pgsql_ddl)
}