'use strict';

const utils = require('@iobroker/adapter-core');
var adapter = utils.Adapter('npemployees');
var mysql = require('mysql');

var Buildings;
var Levels;
var Rooms;
var Employees;
var ioBrokerObjects;

adapter.on("ready", function(err, res)
{
    StartAdapter();
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
                Buildings(connection)
                .then(function(buildings)
                {
                    Levels(connection).then(function(levels)
                    {
                        Rooms(connection).then(function(rooms)
                        {
                            Employees(connection).then(function(employees)
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

async function Buildings(connection)
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
}

async function Levels(connection)
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
}

async function Rooms(connection)
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
}

async function Employees(connection)
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
}

async function DeleteIoBrokerObject(id)
{
    return new Promise((resolve, reject) =>
    {
        adapter.delForeignObject(id, function()
        {
            resolve(id);
        });
    });
}

async function CheckIoBrokerObjects()
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
}

async function ClearIoBrokerObjects()
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
}

async function addIoBrokerObject(id, type, common, native)
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
}

async function SyncBuildings()
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
}

async function SyncLevels()
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
}

async function SyncRooms()
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
}

async function SyncEmployees()
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