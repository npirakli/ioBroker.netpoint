'use strict';

const utils = require('@iobroker/adapter-core');
var adapter = utils.Adapter('npemployees');

/*
var adapter = utils.adapter({
    name: 'npemployees',  // mandatory - name of adapter
    dirname: '',          // optional - path to adapter (experts only)
    systemConfig: false,  // optional - if system global config must be included in object
                          // (content of iobroker-data/iobroker.json)
    config: null,         // optional - alternate global configuration for adapter (experts only)
    instance: null,       // optional - instance of the adapter
    useFormatDate: false, // optional - if adapter wants format date according to global settings.
                          // if true (some libs must be preloaded) adapter can use "formatDate" function.
    logTransporter: false,// optional - if adapter collects logs from all adapters (experts only)

    objectChange: null,   // optional - handler for subscribed objects changes
    message: null,        // optional - handler for messages for this adapter
    stateChange: null,    // optional - handler for subscribed states changes
    ready: null,          // optional - will be called when adapter is initialized
    unload: null,         // optional - will be called by adapter termination
    noNamespace: false    // optional - if true, stateChange will be called with id that has no namespace. Instead "adapter.0.state" => "state"
});
*/

var mysql = require('mysql');

var Buildings;
var Levels;
var Rooms;
var Employees;
var ioBrokerObjects;

var lastChanges;

async function demo()
{
    adapter.log.info("DEMO.----------");
}

adapter.on("ready", function(err, res)
{    
    var updateChecker = setInterval(function()
    {  
        try
        {
            var connection = GetConnection(adapter.config);
            connection.connect();  
          
            connection.query('select dt from np_last_change', function (error, results, fields) 
            {
                connection.end();

                var dt = new Date(results[0].dt);

                if(lastChanges == undefined)
                {
                    StartAdapter();
                }
                else if(results[0].dt > lastChanges)
                {
                    StartAdapter();
                }
                
                lastChanges = new Date(results[0].dt);
                adapter.log.info("npemployees last change: " + lastChanges);            
            });
        }
        catch(ex)
        {
            adapter.log.info("Error: " + ex);     
        }

    }, 5000);
});

adapter.on("message", function(command)
{
    if(command != undefined && command.command == "restart")
    {
        StartAdapter();
    }
});

function StartAdapter()
{
    try 
    {
        var connection = GetConnection(adapter.config);
        connection.connect();                

        connection.query('select 1 + 1 as res', function (error) 
        {
            if(!error)
            {
                var Buildings = function() 
                { 
                    return new Promise((resolve, reject) =>
                    {
                        connection.query('select * from np_building', function (error, results, fields) 
                        {
                            Buildings = results;
                            adapter.log.info("Buildings: " + JSON.stringify(results));
                            resolve(results);
                        });
                    });
                };

                var Levels = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        connection.query('select np_building.name as building_name, np_level.* from np_level left join np_building on np_building.id = np_level.building_id', function (error, results, fields) 
                        {
                            Levels = results;
                            adapter.log.info("Levels: " + JSON.stringify(Levels));
                            resolve(results);
                        });
                    });
                };

                var Rooms = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        connection.query('select np_building.name as building_name, np_level.name as level_name, np_room.* from np_room left join np_building on np_building.id = np_room.building_id left join np_level on np_level.id = np_room.level_id', function (error, results, fields) 
                        {
                            Rooms = results;
                            adapter.log.info("Rooms: " + JSON.stringify(Rooms));
                            resolve(results);
                        });
                    });
                };   

                var Employees = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        connection.query('select np_building.name as building_name, np_level.name as level_name, np_room.name as room_name, np_employee.* from np_employee left join np_building on np_building.id = np_employee.building_id left join np_level on np_level.id = np_employee.level_id left join np_room on np_room.id = np_employee.room_id', function (error, results, fields) 
                        {
                            Employees = results;
                            adapter.log.info("Employees: " + JSON.stringify(Employees));
                            resolve(results);
                        });
                    });
                };

                var CheckIoBrokerObjects = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        adapter.getForeignObjects("npemployees.0.*", "meta", { startkey: 'npemployees.', endkey: 'npemployees.\u9999'}, function(err, results)
                        {
                            ioBrokerObjects = results;
                            adapter.log.info("ioBroker: " + JSON.stringify(ioBrokerObjects));
                            resolve(results);
                        });
                    });
                };

                var DeleteIoBrokerObject = function(id)
                {
                    return new Promise((resolve, reject) =>
                    {
                        adapter.delForeignObject(id, function()
                        {
                            resolve(id);
                        });
                    });
                };

                var ClearIoBrokerObjects = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        adapter.log.info("ioBroker deleting...");
                
                        if(ioBrokerObjects != undefined)
                        {
                            var keys = Object.keys(ioBrokerObjects);
                
                            if(keys != undefined && keys.length > 0)
                            {
                                for(var k = 0; k < keys.length; k++)
                                {
                                    adapter.log.info("ioBroker delete: " + keys[k]);
                
                                    DeleteIoBrokerObject(keys[k]).then(function(res)
                                    {
                                        adapter.log.info("ioBroker deleted: " + res);
                
                                        if((k+1) >= keys.length)
                                        {
                                            resolve(ioBrokerObjects);
                                        }
                                    });
                                }
                            }
                            else
                            {
                                resolve(ioBrokerObjects);
                            }
                        }
                        else
                        {
                            resolve(ioBrokerObjects);
                        }
                    });
                };

                function addIoBrokerObject(id, type, common, native)
                {
                    return new Promise((resolve, reject) =>
                    {
                        var obj = {
                            type:type,
                            common: common,
                            native: native
                        };

                        adapter.setObjectNotExists(id, obj, function()
                        {
                            resolve(obj);
                        });
                    });
                };

                var SyncBuildings = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        if(Buildings != undefined && Buildings.length > 0)
                        {
                            for(var b = 0; b < Buildings.length; b++)
                            {
                                adapter.log.info("Sync Building: " + JSON.stringify(Buildings[b]));
                
                                addIoBrokerObject(Buildings[b].name, "meta", { name: Buildings[b].name, type:'string', role:'value', read:true, write:false }, { _id: Buildings[b].id, _name: Buildings[b].name, np_table: "np_building" }).then(function(obj){ 
                                    adapter.log.info("Building synced: " + JSON.stringify(obj));
                                });
                            }
                        }
                
                        resolve(Buildings);
                    });
                };

                var SyncLevels = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        if(Levels != undefined && Levels.length > 0)
                        {
                            for(var l = 0; l < Levels.length; l++)
                            {
                                adapter.log.info("Sync Level: " + JSON.stringify(Levels[l]));

                                var id = Levels[l].name;

                                if(Levels[l].building_name != undefined)
                                {
                                    id = Levels[l].building_name + "." + Levels[l].name;
                                }

                                addIoBrokerObject(id, "meta", { name: Levels[l].name, type:'string', role:'value', read:true, write:false }, { _id: Levels[l].id, _name: Levels[l].name, _building_id: Levels[l].building_id, np_table: "np_level" }).then(function(obj){ 
                                    adapter.log.info("Level synced: " + JSON.stringify(obj));
                                });
                            }
                        }

                        resolve(Levels);
                    });
                };

                var SyncRooms = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        if(Rooms != undefined && Rooms.length > 0)
                        {
                            for(var r = 0; r < Rooms.length; r++)
                            {
                                adapter.log.info("Sync Room: " + JSON.stringify(Rooms[r]));

                                var id = Rooms[r].name;

                                if(Rooms[r].building_name != undefined && Rooms[r].level_name != undefined)
                                {
                                    id = Rooms[r].building_name + "." + Rooms[r].level_name + "." + Rooms[r].name;
                                }

                                addIoBrokerObject(id, "meta", { name: Rooms[r].name, type:'string', role:'value', read:true, write:false }, { _id: Rooms[r].id, _name: Rooms[r].name, _building_id: Rooms[r].building_id, _level_id: Rooms[r].level_id, np_table: "np_room" }).then(function(obj){ 
                                    adapter.log.info("Room synced: " + JSON.stringify(obj));
                                });
                            }
                        }

                        resolve(Rooms);
                    });
                };

                var SyncEmployees = function()
                {
                    return new Promise((resolve, reject) =>
                    {
                        if(Employees != undefined && Employees.length > 0)
                        {
                            for(var e = 0; e < Employees.length; e++)
                            {
                                adapter.log.info("Sync Employee: " + JSON.stringify(Employees[e]));

                                var name = (Employees[e].firstname + " " + Employees[e].lastname).replace(/\./g,'_');
                                var id = name;

                                if(Employees[e].building_name != undefined && Employees[e].level_name != undefined && Employees[e].room_name != undefined)
                                {
                                    id = Employees[e].building_name + "." + Employees[e].level_name + "." + Employees[e].room_name + "." + name;
                                }

                                addIoBrokerObject(id, "meta", { name: name, type:'string', role:'value', read:true, write:false }, { _id: Employees[e].id, _name: name, _firstname: Employees[e].firstname, _lastname: Employees[e].lastname, _phone: Employees[e].phone, _email: Employees[e].email, _account: Employees[e].account, _building_id: Employees[e].building_id, _level_id: Employees[e].level_id, _room_id: Employees[e].room_id, np_table: "np_employee" }).then(function(obj){ 
                                    adapter.log.info("Employee synced: " + JSON.stringify(obj));
                                });
                            }
                        }

                        resolve(Employees);
                    });
                };

                Buildings()
                .then(function(buildings)
                {
                    Levels().then(function(levels)
                    {
                        Rooms().then(function(rooms)
                        {
                            Employees().then(function(employees)
                            {
                                connection.end();

                                CheckIoBrokerObjects().then(function(objects)
                                {
                                    ClearIoBrokerObjects().then(function(result)
                                    {
                                        SyncBuildings().then(function(syncBuildings)
                                        {
                                            SyncLevels().then(function(syncLevels)
                                            {
                                                SyncRooms().then(function(syncRooms)
                                                {
                                                    SyncEmployees().then(function(syncEmployees)
                                                    {
                                                        Buildings = null;
                                                        Levels = null;
                                                        Rooms = null;
                                                        Employees = null;
                                                        ioBrokerObjects = null;

                                                        adapter.log.info("Data synced");
                                                    });
                                                });
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            }
        });
    } 
    catch (ex)
    {
        return ex;
    }
}

function GetConnection(DbSettings)
{
    var result = mysql.createConnection({
        host: DbSettings.host,
        port: DbSettings.port,
        user: DbSettings.dbuser,
        password: DbSettings.dbpass,
        database: DbSettings.database
    });

    return result;
}