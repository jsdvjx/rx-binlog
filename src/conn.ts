import { Connection, createConnection } from "mysql2"

export interface Option {
    host: string
    port: number
    username: string
    password: string
    database?: string
}
export class Conn {
    private static map = new Map<string, Connection>()
    static create = (option: Option): Connection => {
        const so = JSON.stringify(option);
        return (Conn.map.has(so) ? Conn.map.get(so) : Conn.map.set(so, createConnection({
            host: option.host,
            user: option.username,
            password: option.password,
            port: option.port,
            database: option.database
        })).get(so)) as Connection
    }
}