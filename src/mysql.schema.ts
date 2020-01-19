import * as mysql from 'mysql2'
import { Observable, Observer } from 'rxjs';
import { map, toArray } from 'rxjs/operators'
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
}