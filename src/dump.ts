import { BinlogOption } from "./binlog";
import { spawn, exec, execSync } from "child_process";
import { Observable, Observer, from } from "rxjs";
import { writeFileSync } from "fs";
import { Query } from "./query";
import { Conn } from "./conn";
import { pluck, map, tap, concatMap, mergeMap } from "rxjs/operators";
import { range } from "lodash/fp";
import os from 'os'

export class Dump {
    private path = `${__dirname}/../bin/mysqldump`
    private query = new Query(Conn.create(this.option));
    constructor(private option: BinlogOption) {
        if (os.platform() === 'linux') {
            execSync(`chmod +x ${this.path}`)
        }
    }
    private get dbParam() {
        return [
            `--host=${this.option.host}`,
            `--password=${this.option.password}`,
            `--user=${this.option.username}`
        ]
    }

    insertSql = (option: { src: { table: string, database: string, pk?: string }, dest: string }) => {
        const take = 500000;
        const pk = option.src.pk ? `order by ${option.src.pk} asc` : '';
        return this.query.run(`select count(0)cnt from \`${option.src.database}\`.\`${option.src.table}\``).pipe(
            pluck("cnt"),
            mergeMap((total: number) => {
                const times = Math.ceil(total / take);
                return from(range(0, times).map((page) => [page * take, take]).map(([start, end]) =>
                    `${this.path} ${this.dbParam.join(' ')} -w"true ${pk} limit ${start},${end}" --single-transaction -q --databases ${option.src.database} --tables ${option.src.table}`, { maxBuffer: 1024 * 1024 * 1024 * 2 }
                ));
            }),
            concatMap(cmd => {
                return new Observable<string>((obser: Observer<string>) => {
                    const stream = exec(cmd,
                        { maxBuffer: 1024 * 1024 * 1024 * 4, });
                    let last: string = ''
                    const nextInsertSql = (chunk: string, _last: boolean = false) => {
                        const tmp = (last + chunk).split('\n').filter(i => i.indexOf('INSERT INTO') === 0 && i.length > 10);
                        if (!_last) last = (tmp.pop() || '')
                        for (const sql of tmp) {
                            obser.next(sql.replace(`\`${option.src.table}\``, option.dest).replace(/\\'/g, `''`).replace(/\/\*\![\d]+.+?\*\/;/g, ''))
                        }
                    }
                    stream.stdout?.on('data', (chunk: string) => {
                        nextInsertSql(chunk)
                    })
                    stream.stdout?.on('end', () => {
                        nextInsertSql('', true)
                    })
                    stream.stderr?.on('data', (chunk) => {
                    })
                    stream.on('error', (error) => {
                        obser.error(error);
                    })
                    stream.on('close', () => {
                        obser.complete()
                    })
                })
            })
        )
    }
}