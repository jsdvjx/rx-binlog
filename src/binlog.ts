import { spawn } from "child_process"
import { Observable, Observer, of, zip } from 'rxjs'
import * as mysql from 'mysql2'
import { MysqlSchema, MysqlColumn, MysqlMap } from "./mysql.schema";
import { tap, map, mergeMap, switchMap, toArray } from "rxjs/operators";
import { Query } from "./query";
import { stringify } from "querystring";
import { writeFileSync } from "fs";

export interface EventInfo {
    type: "UPDATE" | "INSERT" | "DELETE" | "UNKOWN",
    database: string,
    table: string,
    pk: string,
    uni: string[]
    emit_at: Date
}
export interface UpdateBody {
    source: Record<string, any>
    update: Record<string, any>
    align: boolean;
    error: boolean
}
export interface ErrorBody {
    error: boolean;
    code: number;
    msg: string;
}
export type InsertBody = { source: Record<string, any>, error: boolean };
export type DeleteBody = { source: Record<string, any>, error: boolean };
export interface BinlogEvent<T extends UpdateBody | InsertBody | DeleteBody | ErrorBody> {
    info: EventInfo;
    body: T
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
    private path = `${process.cwd()}/bin/mysqlbinlog`
    constructor(private option: BinlogOption) { }
    private conn = mysql.createConnection({
        host: this.option.host,
        user: this.option.username,
        password: this.option.password,
        port: this.option.port,
        database: this.option.database
    });
    private ms = new MysqlSchema(this.conn)
    private query = new Query(this.conn)
    private _schema!: MysqlMap;
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
    private static transform = (schema: MysqlMap, event: BinlogEvent<UpdateBody | InsertBody | DeleteBody | ErrorBody>) => {
        if (event.body.error) {
            return event;
        }

        const map = schema[event.info.database] ? schema[event.info.database][event.info.table] : null;

        if (!map) return event;
        const cols = Object.values(map)
        for (const col of cols) {
            if (col.COLUMN_KEY === 'PRI') {
                event.info.pk = col.COLUMN_NAME;
            }
            if (col.COLUMN_KEY === 'PRI') {
                event.info.uni.push(col.COLUMN_NAME)
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
        switch (event.info.type) {
            case 'UPDATE':
                (event.body as UpdateBody).source = resolve((event.body as UpdateBody).source);
                (event.body as UpdateBody).update = resolve((event.body as UpdateBody).update);
                break;
            case 'INSERT':
            case 'DELETE':
                (event.body as DeleteBody).source = resolve((event.body as DeleteBody).source)
                break;
        }
        return event;
    }
    private static check = (input: string) => {
        return ['INSERT', 'DELETE', 'UPDATE'].reduce((res, acc) => {
            return res || input.indexOf(acc) >= 0
        }, false as boolean);
    }
    private static resolve_update = (values: string[] | null): UpdateBody | ErrorBody => {
        if ((values?.length && values.length > 0)) {
            const data = values.reduce((res, acc) => {
                const [name, value] = acc.split('=');
                const target = res[res[0][name] ? 1 : 0];
                target[name] = value;
                return res;
            }, [{}, {}] as [Record<string, string>, Record<string, string>])
            const align = data[0].length === data[1].length;
            const update: Record<string, string> = {};
            if (align) {
                Object.keys(data[0]).map(k => {
                    if (data[0][k] !== data[1][k]) {
                        update[k] = data[1][k];
                    }
                })
            }
            return {
                source: data[0],
                align,
                update: Object.keys(update).length ? update : data[1],
                error: false
            }
        } else {
            return {
                code: 10002,
                msg: 'values error',
                error: true
            }
        }
    }
    private static resolve = (input: string): BinlogEvent<InsertBody | UpdateBody | DeleteBody | ErrorBody> => {
        const _info = /### (?<type>UPDATE|INSERT|DELETE).+\`(?<database>.+?)\`\.\`(?<table>.+?)\`/.exec(input)?.groups;
        if (_info?.type) {
            const info: EventInfo = { ..._info, uni: [], pk: '', emit_at: new Date } as any;
            const values = input.match(/@[\d]{1,2}\=.+/g) || [];
            const body: InsertBody | DeleteBody = { error: false, source: values.reduce((res, acc) => (res[acc.split("=")[0]] = acc.split("=")[1], res), {} as Record<string, string>) }
            switch (info.type) {
                case 'UPDATE':
                    return { info, body: Binlog.resolve_update(values) };
                case 'INSERT':
                    return { info, body }
                default:
                    return { info, body };
            }
        }
        return { info: { table: '', database: '', type: 'UNKOWN', pk: '', uni: [], emit_at: new Date }, body: { error: true, msg: 'unkown', code: 10001 } };
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
        return this.ready.pipe(switchMap(() => new Observable((obser: Observer<BinlogEvent<UpdateBody | InsertBody | DeleteBody | ErrorBody>>) => {
            stream.stdout.on("data", (chunk) => {
                const result = chunk.toString().split('/*!*/;');
                const list = result.filter(Binlog.check).join('\n').split(/BINLOG '[\w\W]+?'/).filter(Binlog.check);
                for (const str of list) {
                    if (str.length) {
                        obser.next(Binlog.transform(this._schema, Binlog.resolve(str)))
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
    autoLog = () => {
        return this.position().pipe(switchMap(this.run))
    }
    private position = () => {
        return this.query.run<MysqlBinlogFile>('show BINARY logs').pipe(
            map(log => (log.Log_name = log.Log_name.replace('mysql-bin.', ''), log)),
            toArray(),
            map(logs => logs.reduce((res, acc) =>
                (res.len = acc.Log_name.length, res.idx = res.idx > parseInt(acc.Log_name) ? res.idx : parseInt(acc.Log_name), res.size = acc.File_size, res)
                , { idx: 0, size: 0, len: 0 } as { idx: number; size: number, len: number })
            ),
            map(position => {
                return [
                    `--start-position=${position.size}`,
                    `mysql-bin.${Array(position.len - position.idx.toString().length + 1).join('0')}${position.idx}`]
            })
        )
    }
}