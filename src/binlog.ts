import { spawn } from "child_process"
import { Observable, Observer, of, zip, Subject } from 'rxjs'
import * as mysql from 'mysql2'
import { MysqlSchema, MysqlColumn, MysqlMap } from "./mysql.schema";
import { tap, map, switchMap, toArray } from "rxjs/operators";
import { Query } from "./query";
import { Conn } from './conn'
import moment, { unix } from "moment";
import { writeFileSync, readFileSync, existsSync } from "fs";
export type SqlType = "UPDATE" | "INSERT" | "DELETE" | "UNKOWN"
export interface BinlogEvent {
    type: SqlType,
    database: string,
    table: string,
    pk: string,
    uni: string[],
    emit_at: Date,
    crc32: string,
    value: string | number
    source: Record<string, any>
    update?: Record<string, any>
    file: string
    position: number
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
    constructor(private option: BinlogOption) { }
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
    private static transform = (schema: MysqlMap, event: BinlogEvent) => {
        const map = schema[event.database] ? schema[event.database][event.table] : null;
        if (!map) return event;
        const cols = Object.values(map)
        for (const col of cols) {
            if (col.COLUMN_KEY === 'PRI') {
                event.pk = col.COLUMN_NAME;
            }
            if (col.COLUMN_KEY === 'UNI') {
                event.uni.push(col.COLUMN_NAME)
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
                        break;
                }

                return (res);
            }, {} as Record<string, Date | number | string | null>)
        }
        switch (event.type) {
            case 'UPDATE':
                event.source = resolve(event.source)
                event.update = resolve(event.update as Record<string, string>)
                break;
            case 'INSERT':
            case 'DELETE':
                event.source = resolve(event.source)
                break;
        }
        event.value = event.source[event.pk] || 0;
        return event;
    }
    private static resolve = (input: string, baseInfo: { type: SqlType, crc32: string, emit_at: Date }): BinlogEvent => {
        const _info = /### (?<type>UPDATE|INSERT|DELETE).+\`(?<database>.+?)\`\.\`(?<table>.+?)\`/.exec(input)?.groups;
        const info: BinlogEvent = { ..._info, uni: [], pk: '', ...baseInfo } as any;
        const reg = /@[\d]{1,2}\=.+/g
        const [source, update] = (info.type === 'UPDATE' ? input.split('### SET').map(i => i.match(reg)) : [input.match(reg)]) || [];
        const toRecord = (values: string[]) => {
            return values.length === 0 ? {} : values.reduce((res, acc) => (res[acc.split("=")[0]] = acc.split("=")[1], res), {} as Record<string, string>)
        }
        info.source = toRecord(source as string[]);
        info.update = toRecord((update || []) as string[]);
        return info
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

    run = (options: string[]) => {
        const stream = spawn(this.path, [
            ...this.baseParam,
            ...this.dbParam,
            ...options
        ]);
        return this.ready.pipe(switchMap(() => new Observable((obser: Observer<BinlogEvent>) => {
            let pool: string = ''
            const map: Record<'Write' | 'Delete' | 'Update', SqlType> = {
                Write: "INSERT",
                Delete: "DELETE",
                Update: "UPDATE"
            }
            const getBlock = () => {
                const match = pool.match(/^#([\d]{6}.+?flags: STMT_END_F)$/m);
                if (match) {
                    const start = match.index;
                    const tmp = match.pop();
                    if (tmp) {
                        const info = /^(?<emit_at>\d{6} [\d:]{8}).+?end_log_pos (?<pos>\d+) CRC32 0x(?<crc32>[a-f\d]{8}).+?(?<type>Update|Write|Delete+?)_/i.exec(tmp)?.groups;
                        if (!info) return null
                        const pos = parseInt(info?.pos)
                        const reg = new RegExp(`^# at ${pos + this.offset}$`, 'm');
                        if (reg.test(pool)) {
                            this.pos = pos
                            pool = pool.substr(start as number)
                            const end = pool.match(reg)?.index
                            if (end) {
                                const block = pool.substr(0, end);
                                pool = pool.substr(end)
                                const list = block.split(/(### INSERT|UPDATE|DELETE )/)
                                list.shift();
                                this.savePosition()
                                return {
                                    info: {
                                        emit_at: moment('20' + info.emit_at, "YYYYMMDD HH:mm:ss").toDate(),
                                        type: map[info.type as keyof typeof map],
                                        crc32: info.crc32
                                    },
                                    list: list.reduce((res, str) => {
                                        if (str[0] === '#') {
                                            res.push(str);
                                        } else {
                                            if (res.length > 0) res[res.length - 1] = res[res.length - 1] + str
                                        }
                                        return res
                                    }, [] as string[]).filter(i => i.indexOf('###') === 0)
                                }
                            }
                        }
                    }
                }
                return null
            }
            stream.stdout.on("data", (chunk) => {
                pool += chunk.toString()
                const positionReg = /Rotate to (?<file>mysql-bin.\d{6})  pos: (?<pos>\d+)$/m;
                writeFileSync('./1.txt', chunk, { flag: 'a' })
                if (positionReg.test(pool)) {
                    const position = positionReg.exec(pool)?.groups
                    this.file = position?.file || ''
                    this.pos = parseInt(position?.pos || '0')
                    const offsetReg = /[\w\W]+end_log_pos (?<pos>\d+) CRC32 0x[a-f\d]{8}.+?Start: binlog[\w\W]+?# at (?<pos_end>\d+)/;
                    this.savePosition()
                    if (offsetReg.test(pool)) {
                        const offset = offsetReg.exec(pool)?.groups as { pos: string, pos_end: string };
                        this.offset = parseInt(offset.pos_end) - parseInt(offset.pos)
                    }
                }
                const { list, info } = getBlock() || { list: [] };
                for (const str of list) {
                    if (str.length) {
                        info && obser.next(Binlog.transform(this._schema, Binlog.resolve(str, info)))
                    }
                }
            })
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
    private savePosition = () => writeFileSync(this.positionPath, JSON.stringify({ file: this.file, pos: this.pos }))
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