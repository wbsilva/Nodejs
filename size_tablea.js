const email = require('../../functions/email');
const seconsole = require('./util/seconsole'); 
const mssql = require('mssql');
const {DB} = require('../../db');
const db = new DB().getInstance();
const pgPO = require('pg');
const intranet_values = require('../../intranet_values');
const connection_values = require('../../connection_values');

var FAILED_CONNECT_DB = [];
var reportText = "";
var schedule = [];

const f = {

    async execute(schedule_name){
        
        var idExecution = Math.random();
        var clientPG1;
        var pool;        
                
        try {
            seconsole.log(idExecution, schedule_name, 'Lista clientes');

            clientPG1 = new pgPO.Client({
                host: connection_values.host,
                database: connection_values.database,
                user: connection_values.user,
                password: connection_values.password,
                port: `5432`                        
            });

            await clientPG1.connect(); 

            //conexão com o DB intranet
            var conf = {
                server: intranet_values.host,
                database: intranet_values.database,
                port: 1433,
                user: intranet_values.user,
                password: intranet_values.password,
                options: {
                    encrypt: false,
                    trustServerCertificate: false
                },

                connectionTimeout: 60000,
                requestTimeout: 60000,
                pool: {
                    idleTimeoutMillis: 60000,
                    max: 1
                } 
            };                
            
            pool = await new mssql.ConnectionPool(conf).connect()            

            // Lista todos os clientes             
            let list_clients = `SELECT e.customername, e.accountid, e.contractid, d.username, d.password, d.dbip, p.version, d.dbtype, e.domain
                                FROM environment_db d, environment e, environment_params p
                                WHERE p.id_environment = e.id
                                    AND d.id_environment = e.id
                                    AND e.deployed = true
                                    AND e.id = 29                                                          
                                ORDER BY e.customername;`            

            let results = await db.query(schedule_name, list_clients); 
           
            for (const row of results.rows){
                if (row.dbtype == "postgres"){                    

                    seconsole.log(idExecution, schedule_name, 'Verificando paramentros do cliente: ' + row.customername);

                    try {

                        clientPG = new pgPO.Client({
                            host: row.dbip,
                            database: row.username,
                            user: row.username,
                            password: row.password,
                            port: `5432`                        
                        });

                        await clientPG.connect();

                        // Lista tamanho da tabela dos clientes                                      
                        let sql = `SELECT current_database() as dbname, schemaname||'.'||tablename AS tablename, pg_total_relation_size(schemaname||'.'||tablename) / 1000 AS tablesizekb
                                    FROM pg_catalog.pg_tables
                                    WHERE schemaname NOT
                                    IN ('pg_catalog', 'information_schema', 'pg_toast')
                                    ORDER BY schemaname, tablename;`;
                        
                        let result = await clientPG.query(sql);                            
                       
                        if(result.rows.length > 0){

                            let insertQueryPO = `INSERT INTO tablesize
                                                (accountid, contractid, dbname, tablename, tablesizekb, date) 
                                                VALUES`;

                            let insertQueryANA = `INSERT INTO [AWSANALYTICS].[dbo].[tablesize]
                                                (accountid, contractid, dbname, tablename, tablesizekb, date) 
                                                VALUES`;                            
                            
                            let rowCount = 0;

                            for (const  rows of result.rows){

                                insertQueryANA += ` ('${row.accountid}', '${row.contractid}', '${rows.dbname}', '${rows.tablename}', ${rows.tablesizekb}, getdate()),`;
                                
                                insertQueryPO += ` ('${row.accountid}', '${row.contractid}', '${rows.dbname}', '${rows.tablename}', ${rows.tablesizekb}, now()),`;
                                
                                rowCount++;
                                
                                // Verifica se o numero de linhas atingiu 1000
                                if (rowCount === 1000){

                                    // Remove a virgula extra no final da consulta
                                    insertQueryPO = insertQueryPO.slice(0, -1);

                                    insertQueryANA = insertQueryANA.slice(0, -1);                                   
                                    
                                    await clientPG1.query(insertQueryPO);

                                    await pool.request().query(insertQueryANA);

                                    // Reinicializa a consulta e o contador
                                    insertQueryANA = `INSERT INTO [AWSANALYTICS].[dbo].[tablesize]
                                                    (accountid, contractid, dbname, tablename, tablesizekb, date) 
                                                    VALUES`;
                                    
                                    insertQueryPO = `INSERT INTO tablesize
                                                    (accountid, contractid, dbname, tablename, tablesizekb, date) 
                                                    VALUES`;
                                    
                                    rowCount = 0;

                                }
                            }   
                            
                            // Verifica se há linhas remanescentes para inserir
                            if (insertQueryANA.length > 0 && insertQueryANA.length !== 218){

                                insertQueryANA = insertQueryANA.slice(0, -1);

                                await pool.request().query(insertQueryANA);

                            }

                            if (insertQueryPO.length > 0 && insertQueryPO.length !== 195){

                                insertQueryPO = insertQueryPO.slice(0, -1);

                                await clientPG1.query(insertQueryPO);

                            }
                        }

                    } catch (e) {

                        //CASO DÊ ERRO DE CONEXAO COM O BANCO, POPULA O ARRAY DE BANCOS INACESSIVEL
                        f.pushFailedConnectDB(row.dbtype, row.dbip, row.domain, e.message);
                        
                    } finally {

                        //GARANTE O CLOSE DA CONNECTION COM O BANCO

                        try{

                            seconsole.log(idExecution, schedule_name, 'Fechando conexão no DB do cliente: ' + row.customername);
                            await clientPG.end();                            

                        } catch (ee){}    
                    } 
                }                
            }             

        } catch (ex) {
            seconsole.log(idExecution, schedule_name, ex);
            console.log(ex);
            
        } finally {  
            if(FAILED_CONNECT_DB.length!=0){
                f.sendEmailReport();  
            }  
            try { 
                seconsole.log(idExecution, schedule_name, 'Close pg PORTALOPS connection...');
                clientPG1.end();
            } catch (e) {}   
            try {    
                seconsole.log(idExecution, schedule_name, 'Close mssql ANALYTICS connection...');
                await pool.close();
            } catch (e) {}               
        }        
    },

    sendEmailReport() {

        (async () => {
            
            try {
                reportText += " - DATABASES COM ERRO CONEXÃO: \n";

                for (reg of FAILED_CONNECT_DB) {
                    reportText += `   + DB_TYPE: ${reg.dbtype} - DB_ip: ${reg.dbip} - DOMAIN: ${reg.domain} - ERROR: ${reg.error}\n`;
                }

                console.log(reportText);

                email.throwSendEmail(reportText, schedule.schedule_name);

            } catch (err){
                
                console.error(err);
                email.throwSendEmail(`Error: ${err.message}`, schedule.schedule_name);
            }
        })();
    },

    pushFailedConnectDB(dbtype, dbip, domain, error) {
        seconsole.log2(schedule, "Failed connect db" + dbtype + " - " + dbip + " - " + domain + " - " + error);
    
        FAILED_CONNECT_DB.push({ 
            'dbtype': dbtype, 
            'dbip': dbip,
            'domain': domain,
            'error': error
        });
    }
}

module.exports = f;
f.execute('teste-ops-ops1612');
