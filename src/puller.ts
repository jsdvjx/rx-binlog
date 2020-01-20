import { Connection } from "mysql2";
import { Query } from "./query";
import { pluck, switchMap, map, take, concatMap, bufferCount, toArray } from "rxjs/operators";
import { Observable, interval, range, concat, merge } from "rxjs";
import { Option, Conn } from "./conn";

export class Puller {
    private conn = Conn.create(this.option)
    constructor(private option: Option) { }
    private query = new Query(this.conn)
    private block = 10000;
    private threads = 5;
    table = (name: string) => {
        return this.count(name).pipe(
            switchMap(total => {
                return range(0, Math.ceil(total / this.block)).pipe(map((offset) => `select * from ${name} limit ${offset * this.block},${this.block}`))
            }),
            bufferCount(this.threads),
            concatMap(sql_list => {
                return merge(...sql_list.map(sql => this.query.run(sql))).pipe(toArray())
            })
        )
    }
    private count = (table: string): Observable<number> => {
        return this.query.run(`select count(0) cnt from ${table}`).pipe(pluck('cnt'));
    }
}