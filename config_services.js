const seconsole = require('./util/seconsole'); 
const pgPO = require('pg');
const mssql = require('mssql');
const {DB} = require('../../db');
const db = new DB().getInstance();
const email = require('../../functions/email');

var FAILED_CONNECT_DB = [];
var reportText = "";
var schedule = [];

const f = {
    
    execute(schedule_name) {

        FAILED_CONNECT_DB = [];
        reportText = "";
        schedule = [];

        var idExecution = Math.random();  
        
        seconsole.log(idExecution, schedule_name,  'Run: ' + schedule_name + ' - Setar configuração pdfconverter');
        
        (async () => {

            var clientPG;  

            schedule = {
                schedule_id_exec: idExecution,
                schedule_name: schedule_name
            };

            try {                

                seconsole.log(idExecution, schedule_name, 'Lista clientes');


                let pdf_condition = `(select s.domain from service s, service_type t
                    where t.type = 'pdf-converter'
                        AND t.id = s.id_service_type
                        AND s.deployed = true
                        AND s.id_service_cluster in (
                            SELECT c.id FROM environment_clusters c
                            WHERE c.region in (
                            SELECT cc.region FROM environment_clusters cc WHERE cc.id = e.id_environment_cluster
                            ) AND c.type_app = 'service'
                        )
                        AND (p.version ~ '^[0-9.]+$')
                        AND (string_to_array(p.version, '.')::int[] >= string_to_array('2.1.8.0', '.')::int[])
                    limit 1)`; 

                let capture_condition = `(select s.domain from service s, service_type t
                    where t.type = 'capture'
                        AND t.id = s.id_service_type
                        AND s.deployed = true
                        AND s.id_service_cluster in (
                            SELECT c.id FROM environment_clusters c
                            WHERE c.region in (
                            SELECT cc.region FROM environment_clusters cc WHERE cc.id = e.id_environment_cluster
                            ) AND c.type_app = 'service'
                        )
                        AND (p.version ~ '^[0-9.]+$')
                        AND (string_to_array(p.version, '.')::int[] >= string_to_array('2.2.0.0', '.')::int[])
                    limit 1)`; 

                let sqllist =  `SELECT e.customername, d.username, d.password, d.dbip, p.version, d.dbtype, e.domain,
                                (SELECT cc.region 
                                    FROM environment_clusters cc 
                                    WHERE cc.id = e.id_environment_cluster) AS region,
                                ${pdf_condition} AS url_pdf,
                                ${capture_condition} AS url_capture
                                FROM environment_db d, environment e, environment_params p
                                WHERE p.id_environment = e.id
                                    AND d.id_environment = e.id
                                    AND e.deployed = true
                                    AND e.id <> 10
                                    AND e.customername not ilike '%testeops%'
                                ORDER BY e.customername;`;
                
                let results = await db.query(schedule_name, sqllist);               

                for ( const row of results.rows){

                    if (row.dbtype === "sql" && (row.url_pdf || row.url_capture)){

                        seconsole.log(idExecution, schedule_name, 'Verificando paramentros e setando configuração no DB do cliente: ' + row.customername);

                        try {

                            var conf = {
                                server: row.dbip,
                                database: row.username,
                                port: 1433,
                                user: row.username,
                                password: row.password,
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

                            const pool = await new mssql.ConnectionPool(conf).connect();
                            
                            if(row.url_pdf){

                                let sql = `select idparam from adparams where cdisosystem=21 and cdparam in (344)`;

                                let resultsmsql = await  pool.request().query(sql);

                                if (resultsmsql.recordset.length == 0){
                                    
                                    let sqlinsertmsql1 = `insert into adparams(cdisosystem, cdparam,idparam) values (21,344,1)`;
    
                                    await  pool.request().query(sqlinsertmsql1);
                                    
                                    let sqlinsertmsql2 = `insert into adparams(cdisosystem, cdparam,nmparam) values (21,345,'https://${row.url_pdf}')`;                     
                                    
                                    await  pool.request().query(sqlinsertmsql2);
    
                                } else {
    
                                    let sqlupdatemsql = `update adparams set idparam=1 where cdparam=344 and cdisosystem=21`;
    
                                    await  pool.request().query(sqlupdatemsql);
    
                                    let sqlupdate2 = `update adparams set nmparam= 'https://${row.url_pdf}' where cdparam=345 and cdisosystem=21`;
                                
                                    await pool.request().query(sqlupdate2);    
    
                                }                                
                            }

                            if(row.url_capture){

                                let sql = `select idparam from adparams where cdisosystem=21 and cdparam in (101)`;
                        
                                let resultsmsql = await  pool.request().query(sql);

                                if (resultsmsql.recordset.length == 0){
                                    
                                    let sqlinsertmsql1 = `insert into adparams(cdisosystem, cdparam,idparam) values (21,101,1)`;

                                    await  pool.request().query(sqlinsertmsql1);  
                                    
                                    let sqlinsertmsql2 = `insert into adparams(cdisosystem, cdparam,nmparam) values (21,102,'https://${row.url_capture}')`;                     
                                    
                                    await  pool.request().query(sqlinsertmsql2);

                                } else {

                                    let sqlupdatemsql = `update adparams set idparam=1 where cdparam=101 and cdisosystem=21`;

                                    await  pool.request().query(sqlupdatemsql);

                                    let sqlupdate2 = `update adparams set nmparam= 'https://${row.url_capture}' where cdparam=102 and cdisosystem=21`;
                                
                                    await pool.request().query(sqlupdate2);


                                }
                            }

                        } catch (e) {

                            //CASO DÊ ERRO DE CONEXAO COM O BANCO, POPULA O ARRAY DE BANCOS INACESSIVEL
                            f.pushFailedConnectDB(row.dbtype, row.dbip, row.domain, e.message);

                        } finally {

                             //GARANTE O CLOSE DA CONNECTION COM O BANCO

                             try{

                                seconsole.log(idExecution, schedule_name, 'Fechando conexão no DB do cliente: ' + row.customername);

                                await pool.close();

                             } catch (ee){}
                        }                                                 
                    }  
                    
                    if (row.dbtype === "postgres" && (row.url_pdf || row.url_capture)){

                        seconsole.log(idExecution, schedule_name, 'Verificando paramentros e setando configuração no DB do cliente: ' + row.customername);
                        
                        try {

                            clientPG = new pgPO.Client({
                                host: row.dbip,
                                database: row.username,
                                user: row.username,
                                password: row.password,
                                port: `5432`                        
                            });

                            await clientPG.connect();

                            if(row.url_pdf){

                                let sql = `select idparam from adparams where cdisosystem=21 and cdparam in (344)`;

                                let result = await clientPG.query(sql);

                                if (result.rows.length === 0){

                                    let sqlinsertpg1 = `insert into adparams(cdisosystem, cdparam,idparam) values (21,344,1)`;
                                    
                                    await clientPG.query(sqlinsertpg1);

                                    let sqlinsertpg2 = `insert into adparams(cdisosystem, cdparam,nmparam) values (21,345,'https://${row.url_pdf}')`;

                                    await clientPG.query(sqlinsertpg2);

                                } else {

                                    let sqlupdatepg1 = `update adparams set idparam=1 where cdparam=344 and cdisosystem=21`;

                                    await clientPG.query(sqlupdatepg1);

                                    let sqlupdatepg2 = `update adparams set nmparam= 'https://${row.url_pdf}' where cdparam=345 and cdisosystem=21`;

                                    await clientPG.query(sqlupdatepg2);

                                }
                            }

                            if(row.url_capture){

                                let sql = `select idparam from adparams where cdisosystem=21 and cdparam in (101)`;
    
                                let result = await clientPG.query(sql);
        
                                if (result.rows.length === 0){
        
                                    let sqlinsertpg1 = `insert into adparams(cdisosystem, cdparam,idparam) values (21,101,1)`;
                                    
                                    await clientPG.query(sqlinsertpg1);
        
                                    let sqlinsertpg2 = `insert into adparams(cdisosystem, cdparam,nmparam) values (21,102,'https://${row.url_capture}')`;
        
                                    await clientPG.query(sqlinsertpg2);
        
                                } else {
        
                                    let sqlupdatepg1 = `update adparams set idparam=1 where cdparam=101 and cdisosystem=21`;
        
                                    await clientPG.query(sqlupdatepg1);
        
                                    let sqlupdatepg2 = `update adparams set nmparam= 'https://${row.url_capture}' where cdparam=102 and cdisosystem=21`;
        
                                    await clientPG.query(sqlupdatepg2);
        
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
            }
            catch (ex) {
                
                seconsole.log(idExecution, schedule_name, ex);
                console.log(ex);

            } finally {  
                if(FAILED_CONNECT_DB.length!=0){
                    f.sendEmailReport();  
                } 
                
            }
        })();
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
