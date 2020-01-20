import * as mysql from 'mysql2'
import { Observable, Observer } from 'rxjs';
import { map, toArray, tap, publishReplay } from 'rxjs/operators'
export type MysqlType = 'varchar' | 'bigint' | 'longtext' | 'datetime' | 'int' | 'tinyint' | 'decimal' | 'double' | 'char' | 'timestamp' | 'set' | 'enum' | 'float' | 'longblob' | 'mediumtext' | 'mediumblob' | 'smallint' | 'text' | 'blob' | 'time' | 'date' | 'mediumint';
export type MysqlColumnKey = 'MUL' | 'PRI' | 'UNI'
export interface MysqlColumn {
    TABLE_SCHEMA: string,
    TABLE_NAME: string,
    COLUMN_NAME: string,
    ORDINAL_POSITION: string,
    DATA_TYPE: MysqlType,
    IS_NULLABLE: "YES" | "NO",
    COLUMN_KEY: MysqlColumnKey,
    CHARACTER_MAXIMUM_LENGTH?: number,
    NUMERIC_PRECISION?: number,
    NUMERIC_SCALE?: number
}
export type MysqlMap = Record<string, Record<string, Record<string, MysqlColumn>>>
export interface Mysql2ddl {

}
export type DdlCreater = (pgsql: { shcema: string, table: string }, mysql: { database: string, table: string }) => Observable<string>;
export class MysqlSchema {
    private sql = `SELECT TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION,DATA_TYPE,IS_NULLABLE,COLUMN_NAME,COLUMN_KEY,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE from information_schema.\`COLUMNS\``;
    constructor(private conn: mysql.Connection) {

    }
    private query = <T = any>() => {
        return new Observable<T>((obser: Observer<T>) => {
            const q = this.conn.query(this.sql).on('result', (db) => {
                obser.next(db as any);
            })
            q.on('error', obser.error.bind(obser))
            q.on('end', obser.complete.bind(obser))
        })
    }
    schema = (): Observable<MysqlMap> => {
        const group = (list: MysqlColumn[], key: keyof MysqlColumn) =>
            list.reduce((res, acc) => {
                //@ts-ignore
                return (res[acc[key]] = res[acc[key]] ? res[acc[key]] : [], res[acc[key]].push(acc), res)
            }, {} as Record<string, MysqlColumn[]>)
        const dbGroup = (list: MysqlColumn[]) => {
            return Object.fromEntries(Object.entries(group(list, 'TABLE_SCHEMA')).map(([dbName, columns]) => {
                return [dbName, Object.fromEntries(Object.entries(group(columns, 'TABLE_NAME')).map(([tabName, _columns]) => {
                    return [tabName, _columns.reduce((res, col) => {
                        return (res[`@${col.ORDINAL_POSITION}`] = col, res)
                    }, {} as Record<string, MysqlColumn>)]
                }))]
            }))
        }
        return this.query<MysqlColumn>().pipe(
            toArray(),
            map(dbGroup)
        )
    }
    private static m2p_type = (column: MysqlColumn) => {
        switch (column.DATA_TYPE) {
            case 'varchar':
                return `varchar(${column.CHARACTER_MAXIMUM_LENGTH || 255})`
            case 'date':
            case 'time':
                return column.DATA_TYPE;
            case 'timestamp':
            case 'datetime':
                return 'timestamp';
            case 'bigint':
                return 'int8';
            case 'int':
            case 'mediumint':
                return 'int4'
            case 'smallint':
            case 'tinyint':
                return 'int2';
            case 'decimal':
            case 'float':
                return 'numeric' + (column.NUMERIC_PRECISION ? `(${column.NUMERIC_PRECISION},${column.NUMERIC_SCALE || 2})` : '');
            default:
                return 'varchar(255)'
        }
    }
    pgsql_ddl: DdlCreater = (pgsql, mysql) => {
        return this.schema().pipe(
            map(s =>
                s[mysql.database][mysql.table]
            ),
            map(tableSchema => {
                const columns = Object.values(tableSchema);
                const pkc = columns.filter(col => col.COLUMN_KEY === 'PRI').pop()
                const types = columns.sort((a, b) => parseInt(a.ORDINAL_POSITION) - parseInt(b.ORDINAL_POSITION)).map(col => {
                    return `    ${col.COLUMN_NAME} ${MysqlSchema.m2p_type(col)} ${col.IS_NULLABLE ? '' : 'NOT NULL'}`
                }).join(',\n') +"\n    "+ ((pkc && pkc?.COLUMN_NAME) ? `,PRIMARY KEY("${pkc?.COLUMN_NAME}")` : '');
                return `CREATE TABLE "${pgsql.shcema}"."${pgsql.table}" (\n${types})`
            })
        )
    }
}