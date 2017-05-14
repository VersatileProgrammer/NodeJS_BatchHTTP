var request = require('request');
var async   = require('async');
var fs      = require('fs');
var util    = require('util');
var jsonfile= require('jsonfile');
var utility = require('./lib/utility');

// ===== Global ===== 
var CONSTANTS = {
    // Source APIs
    srcURL:             "", // customer ids
    spotifySongAPI:     "https://open.spotify.com/track/",
    spotifyTrackAPI:    "https://api.spotify.com/v1/tracks/",
    spotifyArtistAPI:   "https://api.spotify.com/v1/artists/",

    // Destination APIs
    baseURL:            "", 
    postURL:            "", 
    auth:               "",

    // General options for API call
    timeOut:            10000,
    timeRetryAfter:     5000, // ms
    retryMaxLevel:      5, // 1: 5000ms, 2: 10000ms, 3: 15000ms, 4: 20000ms, 5: 25000ms

    // Numbers of calls in one parallel operation
    onetimeAPILimit:    100, 
    spotifyBunchCount:  50,    // maximum number of spotify bunch API call for track/artist
    spotifyAPILimit:    20,
    artistThreshold:    2,

    // Compact Threshold
    compactThreshold:   40,

    // Caching data
    cacheArtist:        "./cache/artists/",   // cache/artist
    cacheTrack:         "./cache/tracks/",    // cache/track
    cacheIndexFile:     "index.json",
    cacheType_Track:    "MusicTracks",
    cacheType_Artist:   "MusicArtists",
    cacheFilesInFolder: 1000
};

const SECTION_ALL       = "all";
const SECTION_LIKES     = "likes";
const SECTION_GENDER    = "gender";
const SECTION_MUSIC     = "music";

const TYPE_EVENT        = "event";
const TYPE_BRAND        = "brand";

var g_args = {
    id:             "0",            // eventID /
    type:           TYPE_EVENT,     // event / brand, ex: event = https://www.theticketfairy.com/api/customer-ids/623, brand = https://www.theticketfairy.com/api/customer-ids/138/?type=bran  
    section:        SECTION_ALL,    // all / music / likes / gender,
    includeIDs:     "true",         // true: customerIDs in json, false: customerIDs not included in json
    log:            "true"          // true: dump log to log/type-id-section-includeIDs-timestamp.log
}

var g_data = {
    // customerIDs from CONSTANTS.srcURL
    customerIDArray:            [],
    totalCustomerCount:         0, 
    currentPos:                 0,
    retryLevel:                 0,

    // variables to be stored in json
    maleCount:                  0,
    femaleCount:                0,
    
    likesArray:                 [],
    likesArrayIndex:            {},
    likesArrayCompacted:        [],

    currentTrackPos:            0,
    tracksArray:                [],
    tracksArrayIndex:           {},
    tracksArrayNeedFetch:       [],
    artistsArray:               [],
    artistsArrayIndex:          {},
    musicStreamingArray:        [],
    musicStreamingArrayIndex:   {},
    musicStreamingArrayNeedFetch:   [],
    musicStreamingArrayCompacted:   [],
    musicApps:                  {},
    currentSpotifyPos:          0,

    // variables for jsoncaching
    tracks_loadedCacheArray:    {},
    tracks_newCacheArray:       {},
    artists_loadedCacheArray:   {},
    artists_newCacheArray:      {},

    // variables for showing result
    fetchSuccessed:             [],
    fetchFailed:                [],
    fetchNotFound:              [],
    totalLikesLoaded:           0,   
    totalTracksLoaded:          0,
    fetchArtistSuccessed:       [],
    fetchArtistFailed:          [],

    // time elapsed calculation
    time_total:                 0,
    time_step2:                 0, // Loading customer ids from array
    time_step21:                0, // Loading cached JSON (artists, tracks)
    time_step31:                0, // Fetch customers from Cloudant API
    time_step32:                0, // Fetch artists from Spotify API
    time_step33:                0, // Calculate artist fan count
    time_step34:                0, // Fetch spotify artist images
    time_step5:                 0, // cacheToJSON 
    time_step6:                 0, // Post compacted to DB 
    time_step7:                 0, // Post to DB 
}

// ===== Block 0: Main Function =====
main();

function printLog(str){
    console.log(str);
}

function main(){
    var t = process.hrtime();
    async.waterfall([
        function(callback) {
            // STEP 1: Process arguments and put result to g_args  
            printLog('STEP 1: Process arguments and put result to g_args');          
            processArguments(callback);
        },
        function(callback) {
            // STEP 2: Load customerIDs
            printLog('STEP 2: Load customerIDs');            
            loadCustomerIDs(callback);
        },
        function(callback) {
            // STEP 2.1: Load cached JSON
            printLog('STEP 2.1: Load cached JSON');            
            loadCachedJSON(callback);
        },
        function(callback){
            // STEP 3.1: Fetch customers from Cloudant API  
            printLog('STEP 3.1: Fetch customers from Cloudant API');
            // g_data.totalCustomerCount = 100;
            fetchCustomers(callback);
        },
        function(callback){
            // STEP 3.2: Fetch artists from Spotify API
            printLog('STEP 3.2: Fetch artists from Spotify API');
            fetchArtists(callback);
        },
        function(callback){
            // STEP 3.3: Calculate artist fan count
            printLog('STEP 3.3: Calculate artist fan count');
            calculateArtistFanCount(callback);
        },
        function(callback){
            // STEP 3.4: Fetch spotify artist images
            printLog('STEP 3.4: Fetch spotify artist images');
            fetchSpotifyArtistImages(callback);
        },
        function(callback){
            // STEP 4: Print result
            printLog('STEP 4: Print result');
            printResult(callback);
        },
        function(callback){
            // STEP 5: Cache API call
            printLog('STEP 5: Cache API call');
            cacheToJSON(callback);
        },
        function(callback){
            // STEP 6: Post compacted(filtered by threshold) to DB
            printLog('STEP 6: Post compacted(filtered by threshold) to DB');
            postCompactedToDB(callback);
        },
        function(callback){            
            // STEP 7: Post to DB
            printLog('STEP 7: Post to DB');
            postToDB(callback);
        }
    ], function (err, result) {
        g_data.time_total = process.hrtime(t)[0];
        printLog('Total Time Elapsed: ' + g_data.time_total + '(s)');
        if(err){
            printLog('Error Occurred: ', result);
            return;
        }else{
            printLog('Successfully finished!');
            return;
        }
    });
}

// ===== STEP 1: Process arguments and put result to g_args =====
function processArguments(callback){
    // process arguments
    for(var i = 0; i < process.argv.length; i++){
        var arg = process.argv[i];
        if(arg.indexOf('=') >= 0){
            var param = arg.split('=');
            g_args[param[0]] = param[1];
        }
    }

    if(g_args.log == "true"){
        // configure output
        var dir = __dirname + '/log';
        utility.checkDirectorySync(dir);
        var timestamp = new Date().toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '').replace(/-/g, '');
        var path = dir + '/' + g_args.type + '-' + g_args.id + '-' + g_args.section + '-' + g_args.includeIDs + '-' + (timestamp) + '.log';
        var log_file = fs.createWriteStream(path, {flags : 'w'});
        var log_stdout = process.stdout;
        console.log = function(d) { //
          log_file.write(util.format(d) + '\n');
          log_stdout.write(util.format(d) + '\n');
        };
    }
    // pass to next step
    callback();
}

// ===== STEP 2: Loading customer ids from array =====
function loadCustomerIDs(callback){
    var srcURL = CONSTANTS.srcURL + g_args.id + (g_args.type == TYPE_EVENT ? '' : '/?type=brand');
    printLog(' From: ' + srcURL);
    var t = process.hrtime();

    request(srcURL, function (err, resp, body) {
        if (!err && resp.statusCode === 200) {
            var resultAsJSON = JSON.parse(body);   
            g_data.customerIDArray = resultAsJSON.data ? resultAsJSON.data.customerIds : [];
        }else{
            g_data.time_step2 = process.hrtime(t)[0];
            printLog(' Time Elapsed: ' + g_data.time_step2 + '(s)');
            
            callback(true, 'STEP 2 - HTTPRequest failed');
            return;
        }
        g_data.totalCustomerCount = g_data.customerIDArray ? g_data.customerIDArray.length : 0;        
        printLog(' Loaded customers: ' + g_data.totalCustomerCount);
        
        g_data.time_step2 = process.hrtime(t)[0];
        printLog(' Time Elapsed: ' + g_data.time_step2 + '(s)');

        if(g_data.totalCustomerCount > 0){
            callback();
        }else{
            callback(true, 'STEP 2 - Loaded customers: 0');
        }
    });
}

// ===== STEP 2.1: Load cached JSON =====
function loadCachedJSON(callback){
    var t = process.hrtime();

    g_data.tracks_loadedCacheArray = loadCachedData(CONSTANTS.cacheType_Track);
    g_data.artists_loadedCacheArray = loadCachedData(CONSTANTS.cacheType_Artist);    

    g_data.time_step21 = process.hrtime(t)[0];
    printLog(' Time Elapsed: ' + g_data.time_step21 + '(s)');
    callback();
}

function loadCachedData(type){
    var path = type == CONSTANTS.cacheType_Track ? CONSTANTS.cacheTrack + CONSTANTS.cacheIndexFile : CONSTANTS.cacheArtist + CONSTANTS.cacheIndexFile;
    var cacheArray = utility.loadJSONtoObject(path);
    if(cacheArray != null){
        printLog(' -' + type + ':Path= ' + path + ', Count=' + cacheArray.count + ', Data Count=' + Object.keys(cacheArray.data).length);
        return cacheArray;
    }else{
        printLog(' -' + type + ':Not Exist');
        return {
            count: 0,
            data: {}
        }
    }
}

// ===== STEP 3.1: Fetch customers from Cloudant API =====
function fetchCustomers(callback){
    var t = process.hrtime();
    async.whilst(
        function test() { 
            return g_data.currentPos < g_data.totalCustomerCount; 
        },
        fetchCustomer_consequence,
        function (err) {
            // Move to next step - print result
            g_data.time_step31 = process.hrtime(t)[0];
            printLog(' Time Elapsed: ' + g_data.time_step31 + '(s)');
            callback();
        }
    );
}

function fetchCustomer_consequence(next) {
    var start = g_data.currentPos;
    var end = g_data.currentPos + CONSTANTS.onetimeAPILimit > g_data.totalCustomerCount ? g_data.totalCustomerCount : g_data.currentPos + CONSTANTS.onetimeAPILimit;
    var sliced = g_data.customerIDArray.slice(start, end);
    if(sliced.length == 0){
        next();
        return;
    } 
    printLog(' Loading: ' + sliced.length);

    var sliced1 = [];
    for(var i = 0; i<sliced.length; i++){
        sliced1.push({
            id: sliced[i],
            successed: false // false: error, true: successed
        });
    }
    g_data.retryLevel = 0;
    fetchCustomer_block(sliced1, next);    
}

function fetchCustomer_block(array, callback){
    async.forEachOf(array,
      fetchCustomer_parallel,
      function(err){
        var successed = true;
        var newArray =[];
        for(i = 0; i< array.length; i++){
            if(!array[i].successed){
                successed = false;
                newArray.push(array[i]);
            }
        }
        if(successed){
            callback();
        }else if(g_data.retryLevel >= CONSTANTS.retryMaxLevel){
            for(i = 0; i<newArray.length;i++)
                g_data.fetchFailed.push(newArray[i].id);
            callback(); // terminate execution and move to parent.
        }
        else{
            g_data.retryLevel++;
            printLog('  Failed: ' + newArray.length);
            var retryAfter = CONSTANTS.timeRetryAfter * g_data.retryLevel;
            printLog('  Retry after: ' + retryAfter + 'ms in level: ' + g_data.retryLevel);
            setTimeout(function(){
                fetchCustomer_block(newArray, callback);
            }, retryAfter);            
        }
      }
    );
}

function fetchCustomer_parallel(item, key, callback){
    var id = item.id;    
    async.waterfall([
        function(subcallback) {
            var options = {
                url: CONSTANTS.baseURL + id,
                timeout: CONSTANTS.timeOut,
                headers:{
                    'Authorization': CONSTANTS.auth
                }
            };
            request(options, function (err, resp, body) {
                if (!err && resp.statusCode === 200) {
                    resp.setEncoding('utf8');
                    var resultAsJSON = JSON.parse(body); 
                    item.successed = true;
                    subcallback(null, resultAsJSON);
                }else{
                    if(!err){
                        item.successed = true;
                        g_data.fetchNotFound.push(id);
                        printLog('  No.' + (g_data.currentPos + 1) + ', CustomerID: ' + id + ', Not Found');
                    }else{
                        item.successed = false;
                        // g_data.fetchFailed.push(id);
                        printLog('  No.' + (g_data.currentPos + 1) + ', CustomerID: ' + id + ', Error:' + err + ': ' + utility.filterLineBreak(body));
                    }                    
                    subcallback(true, '');
                }
            });            
        },
        function(json, subcallback){
            var loadedLikes = json.likes ? json.likes.length : 0;
            var loadedMusics = json.music_listens ? json.music_listens.length : 0;

            var isMale = json.profile ? (json.profile.gender === "male" ? true : false ) : false;
            if(isMale)
                g_data.maleCount++;
            else
                g_data.femaleCount++;
            g_data.totalLikesLoaded += loadedLikes;
            g_data.totalTracksLoaded += loadedMusics;
            g_data.fetchSuccessed.push(id);

            printLog('  No.' + (g_data.currentPos + 1) + ', CustomerID: ' + id + ', Gender: ' + (isMale ? "male" : "female") + ', Likes: ' + loadedLikes + ', Musics: ' + loadedMusics);
            
            if(g_args.section == SECTION_ALL || g_args.section == SECTION_LIKES){
                appendToLikes(json.likes);
            }
            if(g_args.section == SECTION_ALL || g_args.section == SECTION_MUSIC){
                appendToTracks(json.music_listens, id);
            }
            subcallback();
        }
    ], function (err, result) {
        if(g_data.retryLevel == 0)
            g_data.currentPos += 1;
        callback();
    });
}

function appendToLikes(likes){
    if(!likes || likes.length == 0)
        return;
    for(var i = 0; i < likes.length; i++){
        var like = likes[i];
        var object = {
            id: like.id,
            name: like.name,
            category : like.category,
            count: 1
        };

        var index = g_data.likesArrayIndex[object.id];
        if(index === undefined) { // if not exist
            index = g_data.likesArray.length;
            g_data.likesArrayIndex[object.id] = index;
            g_data.likesArray[index] = object;
        }else{ // duplicated
            //printLog('duplicated!', g_data.likesArray[index], object);
            g_data.likesArray[index].count++;
        }
    }
}

function appendToTracks(music_listens, customerID){
    if(!music_listens || music_listens.length == 0)        
        return;
    for(var i = 0; i < music_listens.length; i++){
        var music_listen = music_listens[i];
        if(music_listen && music_listen.application && music_listen.application.name){
            var appName = music_listen.application.name;
            var index = g_data.musicApps[appName];
            if(index === undefined){
                g_data.musicApps[appName] = 1;
            }else{
                g_data.musicApps[appName]++;
            }
        }
        if(!music_listen || !music_listen.application || music_listen.application.name != "Spotify" || !music_listen.data || !music_listen.data.song ){
            // not spotify
        }else{ // only spotify
            var songURL = music_listen.data.song.url;
            var trackID = songURL.replace('http:', 'https:').replace(CONSTANTS.spotifySongAPI, '');
            var object = {
                trackID: trackID,
                custIDArray: [],
                custIDArrayIndex: {},
                count: 1
            }
            object.custIDArray[0] = customerID;
            object.custIDArrayIndex[customerID] = 0;

            var index = g_data.tracksArrayIndex[trackID];
            if(index === undefined) { // is not exist
                // add to array
                index = g_data.tracksArray.length;
                g_data.tracksArray[index] = object;
                g_data.tracksArrayIndex[trackID] = index;
            }else{
                // already exist
                var track = g_data.tracksArray[index];
                // check if customerID is in array
                var indexCustID = track.custIDArrayIndex[customerID];
                if(indexCustID === undefined){ // is not exist
                    // add to array
                    indexCustID = track.custIDArray.length;
                    track.custIDArray[indexCustID] = customerID;
                    track.custIDArrayIndex[customerID] = indexCustID;
                }
                track.count++;
            }
        }
    }
}

// ===== STEP 3.2: Fetch artists from Spotify API =====
function fetchArtists(callback){
    if(!(g_args.section == SECTION_ALL || g_args.section == SECTION_MUSIC)){
        callback();
        return;
    }
    printLog(' TotalCount: ' + g_data.tracksArray.length);

    fetchArtistsFromCached();

    var t = process.hrtime();
    async.whilst(
        function test() { 
            return g_data.currentTrackPos < g_data.tracksArrayNeedFetch.length; 
        },
        fetchArtists_consequence,
        function (err) {
            // Move to next step - print result
            g_data.time_step32 = process.hrtime(t)[0];
            printLog(' Time Elapsed: ' + g_data.time_step32 + '(s)');
            callback();
        }
    );
}

function fetchArtistsFromCached(){
    g_data.tracksArrayNeedFetch = [];
    var t = process.hrtime();
    var totalCount = g_data.tracksArray.length;    
    var loadFromCached = [];
    for(var i=0; i<totalCount; i++){
        var trackObj = g_data.tracksArray[i];
        var trackID = trackObj.trackID;
        var index = g_data.tracks_loadedCacheArray.data[trackID];
        if(index === undefined) { // is not in cached
            g_data.tracksArrayNeedFetch.push(trackObj);
        }else{
            var path = CONSTANTS.cacheTrack + index + '/' + trackID + '.json';
            var cache = utility.loadJSONtoObject(path);
            if(cache == null){ // failed to load
                g_data.tracksArrayNeedFetch.push(trackObj);
            }else{
                loadFromCached.push(cache);
                g_data.fetchArtistSuccessed.push(trackID);
            }
        }
    }
    appendToArtists(loadFromCached, false);
    printLog(' - Load from Cache: ' + loadFromCached.length + ', Time Elapsed: ' + process.hrtime(t)[0] + '(s)');
    printLog(' - Need Fetch: ' + g_data.tracksArrayNeedFetch.length);
}

function fetchArtists_consequence(next){
    var start = g_data.currentTrackPos;
    var step = CONSTANTS.spotifyAPILimit * CONSTANTS.spotifyBunchCount;
    var end = g_data.currentTrackPos + step > g_data.tracksArrayNeedFetch.length ? g_data.tracksArrayNeedFetch.length : g_data.currentTrackPos + step;

    var sliced = g_data.tracksArrayNeedFetch.slice(start, end);
    if(sliced.length == 0){
        next();
        return;
    }
    printLog('  Loading: ' + sliced.length + ', start: ' + start);

    // prepare bunch trackAPI call
    var sliced1 = [];
    var ids = [];
    var count = 0;
    for(var i = 0; i < sliced.length; i++){
        ids.push(sliced[i].trackID);
        count++;
        if(count >= CONSTANTS.spotifyBunchCount || (i == sliced.length - 1)){
            var param = ids.join(",");
            var obj = {
                ids: param,
                successed: false // false: error, true: successed
            };
            sliced1.push(obj);
            ids = [];
            count = 0;
        }
    }
    // console.log(sliced1);
    g_data.retryLevel = 0;
    fetchArtists_block(sliced1, next);    
}

function fetchArtists_block(array, callback){
    async.forEachOf(array,
      fetchArtists_parallel,
      function(err){
        var successed = true;
        var newArray =[];
        for(i = 0; i< array.length; i++){
            if(!array[i].successed){
                successed = false;
                newArray.push(array[i]);
            }
        }
        if(successed){
            callback();
        }else if(g_data.retryLevel >= CONSTANTS.retryMaxLevel){
            for(i = 0; i<newArray.length;i++){
                var ids = newArray[i].ids;
                var idArray = ids.split(",");
                for(var j = 0; j<idArray.length; j++)
                    g_data.fetchArtistFailed.push(idArray[j]);
            }
            callback(); // terminate execution and move to parent.
        }else{
            g_data.retryLevel++;
            printLog('  Failed: ' + newArray.length);
            var retryAfter = CONSTANTS.timeRetryAfter * g_data.retryLevel;
            printLog('  Retry after: ' + retryAfter + 'ms in level: ' + g_data.retryLevel);
            setTimeout(function(){
                fetchArtists_block(newArray, callback);
            }, retryAfter);            
        }
      }
    );
}

function fetchArtists_parallel(item, key, callback){
    var tracksURL = CONSTANTS.spotifyTrackAPI + "?ids=" + item.ids;
    var idArray = item.ids.split(",");
    var count = idArray.length;
    async.waterfall([
        function(subcallback) {
            var options = {
                url: tracksURL,
                timeout: CONSTANTS.timeOut
            };
            request(options, function (err, resp, body) {
                if (!err && resp.statusCode === 200) {
                    resp.setEncoding('utf8');
                    var resultAsJSON = JSON.parse(body); 
                    item.successed = true;
                    subcallback(null, resultAsJSON);
                }else{
                    item.successed = false;
                    printLog('  TracksURL: "' + tracksURL + '" Error: ' + err + ': ' + utility.filterLineBreak(body));
                    // for(var i = 0; i < idArray.length; i++)
                    //     g_data.fetchArtistFailed.push(idArray[i]);
                    subcallback(true, '');
                }
            });
        },
        function(json, subcallback){
            for(var i = 0; i < idArray.length; i++)
                g_data.fetchArtistSuccessed.push(idArray[i]);
            appendToArtists(json.tracks, true);
            subcallback();
        }
    ], function (err, result) {
        if(g_data.retryLevel == 0)
            g_data.currentTrackPos += count;
        callback();
    });
}

function appendToArtists(tracks, saveCache){
    if(!tracks || tracks.length == 0)
        return;
    for(var ti = 0; ti < tracks.length; ti++){
        if(tracks[ti] && tracks[ti].artists){
            var artists = tracks[ti].artists;
            var trackID = tracks[ti].id;
            if(tracks[ti].linked_from) // same contents for different id
                trackID = tracks[ti].linked_from.id;
            var cachedArtists = []; // for caching
            for(var i = 0; i < artists.length; i++){
                var artist = artists[i];
                var artistID = artist.id;
                var object = {
                    id: artistID,
                    name: artist.name,
                    application: 'Spotify',
                    href: artist.href,
                    trackIDArray: [],
                    trackIDArrayIndex: {},
                    customerFanArray: {},
                    count: 1,
                };
                if(saveCache){
                    var cached = {
                        id: artistID,
                        name: artist.name,
                        application: 'Spotify',
                        href: artist.href
                    };
                    cachedArtists.push(cached);
                }
                object.trackIDArray[0] = trackID;
                object.trackIDArrayIndex[trackID] = 0;

                var index = g_data.artistsArrayIndex[artistID];
                if(index === undefined) { // is not exist
                    // add to array
                    index = g_data.artistsArray.length;
                    g_data.artistsArray[index] = object;
                    g_data.artistsArrayIndex[artistID] = index;                    
                }else{
                    // already exist
                    var artist = g_data.artistsArray[index];
                    // check if trackID is in array
                    var indexTrackID = artist.trackIDArrayIndex[trackID];
                    if(indexTrackID === undefined){
                        // add to array
                        indexTrackID = artist.trackIDArray.length;
                        artist.trackIDArray[indexTrackID] = trackID;
                        artist.trackIDArrayIndex[trackID] = indexTrackID;
                    }
                    artist.count++;
                }
            }
            // for caching
            if(saveCache){
                g_data.tracks_newCacheArray[trackID] = {
                    id: trackID,
                    artists: cachedArtists
                };
            }            
        }     
    }    
}

// ===== STEP 3.3: Calculate artist fan count =====
function calculateArtistFanCount(callback){
    var t = process.hrtime();

    // calculate customerFanArray
    for(var i = 0; i < g_data.artistsArray.length; i++){
        var artist = g_data.artistsArray[i];
        for(var j = 0; j < artist.trackIDArray.length; j++){
            var trackID = artist.trackIDArray[j];
            var trackIndex = g_data.tracksArrayIndex[trackID];
            if(trackIndex !== undefined){
                var track = g_data.tracksArray[trackIndex];
                for(var k = 0; k < track.custIDArray.length; k++){
                    var custID = track.custIDArray[k];
                    var searchCustomer = artist.customerFanArray[custID];
                    if(searchCustomer === undefined){
                        artist.customerFanArray[custID] = 1;
                    }else{
                        artist.customerFanArray[custID]++;
                    }
                }
            }
        }
    }
    // console.log(g_data.artistsArray);
    // create new array
    for(i = 0; i < g_data.artistsArray.length; i++){
        var artist = g_data.artistsArray[i];
        var fanCount = 0;
        for (var key in artist.customerFanArray) {
            var count = artist.customerFanArray[key];
            if(count >= CONSTANTS.artistThreshold){
                fanCount++;
            }
        }
        if(fanCount > 0){
            var object = {
                id: artist.id,
                name: artist.name,
                application: 'Spotify',
                href: artist.href,
                count: fanCount
            }
            var index = g_data.musicStreamingArray.length;
            g_data.musicStreamingArray[index] = object;
            g_data.musicStreamingArrayIndex[artist.id] = index;
        }        
    }

    // Move to next step - fetch spotify artist images
    g_data.time_step33 = process.hrtime(t)[0];
    printLog(' Time Elapsed: ' + g_data.time_step33 + '(ms)');
    callback();
}

// ===== STEP 3.4: Fetch spotify artist images =====
function fetchSpotifyArtistImages(callback){
    if(!(g_args.section == SECTION_ALL || g_args.section == SECTION_MUSIC)){
        callback();
        return;
    }
    printLog(' TotalCount: ' + g_data.musicStreamingArray.length);

    fetchArtistImagesFromCached();

    var t = process.hrtime();
    async.whilst(
        function test() { 
            return g_data.currentSpotifyPos < g_data.musicStreamingArrayNeedFetch.length; 
        },
        fetchSpotifyArtist_consequence,
        function (err) {
            // Move to next step - print result
            g_data.time_step34 = process.hrtime(t)[0];
            printLog(' Time Elapsed: ' + g_data.time_step34 + '(s)');
            callback();
        }
    );
}

function fetchArtistImagesFromCached(){
    g_data.musicStreamingArrayNeedFetch = [];
    var t = process.hrtime();
    var totalCount = g_data.musicStreamingArray.length;    
    var loadFromCached = [];
    for(var i=0; i<totalCount; i++){
        var artistObj = g_data.musicStreamingArray[i];
        var artistID = artistObj.id;
        var index = g_data.artists_loadedCacheArray.data[artistID];
        if(index === undefined) { // is not in cached
            g_data.musicStreamingArrayNeedFetch.push(artistObj);
        }else{
            var path = CONSTANTS.cacheArtist + index + '/' + artistID + '.json';
            var cache = utility.loadJSONtoObject(path);
            if(cache == null){ // failed to load
                g_data.musicStreamingArrayNeedFetch.push(artistObj);
            }else{
                loadFromCached.push(cache);
            }
        }
    }
    appendImagesToArtists(loadFromCached, false);
    printLog(' - Load from Cache: ' + loadFromCached.length + ', Time Elapsed: ' + process.hrtime(t)[0] + '(s)');
    printLog(' - Need Fetch: ' + g_data.musicStreamingArrayNeedFetch.length);
}

function fetchSpotifyArtist_consequence(next) {
    var start = g_data.currentSpotifyPos;
    var step = CONSTANTS.spotifyAPILimit * CONSTANTS.spotifyBunchCount;
    var end = g_data.currentSpotifyPos + step > g_data.musicStreamingArrayNeedFetch.length ? g_data.musicStreamingArrayNeedFetch.length : g_data.currentSpotifyPos + step;
    
    var sliced = g_data.musicStreamingArrayNeedFetch.slice(start, end);
    if(sliced.length == 0){
        next();
        return;
    } 
    printLog('  Loading: ' + sliced.length + ', start: ' + start);

    // prepare bunch trackAPI call
    var sliced1 = [];
    var ids = [];
    var count = 0;
    for(var i = 0; i < sliced.length; i++){
        ids.push(sliced[i].id);
        count++;
        if(count >= CONSTANTS.spotifyBunchCount || (i == sliced.length - 1)){
            var param = ids.join(",");
            var obj = {
                ids: param,
                successed: false // false: error, true: successed
            };
            sliced1.push(obj);
            ids = [];
            count = 0;
        }
    }
    // console.log(sliced1);
    g_data.retryLevel = 0;
    fetchSpotifyArtist_block(sliced1, next);
}

function fetchSpotifyArtist_block(array, callback){
    async.forEachOf(array,
      fetchSpotifyArtist_parallel,
      function(err){
        var successed = true;
        var newArray =[];
        for(i = 0; i< array.length; i++){
            if(!array[i].successed){
                successed = false;
                newArray.push(array[i]);
            }
        }
        if(successed || g_data.retryLevel >= CONSTANTS.retryMaxLevel){
            callback(); // terminate execution and move to parent.
        }
        else{
            g_data.retryLevel++;
            printLog('  Failed: ' + newArray.length);
            var retryAfter = CONSTANTS.timeRetryAfter * g_data.retryLevel;
            printLog('  Retry after: ' + retryAfter + 'ms in level: ' + g_data.retryLevel);
            setTimeout(function(){
                fetchSpotifyArtist_block(newArray, callback);
            }, retryAfter);            
        }
      }
    );
}

function fetchSpotifyArtist_parallel(item, key, callback){
    var artistsURL = CONSTANTS.spotifyArtistAPI + "?ids=" + item.ids;
    var idArray = item.ids.split(",");
    var count = idArray.length;
    async.waterfall([
        function(subcallback) {
            var options = {
                url: artistsURL,
                timeout: CONSTANTS.timeOut
            };
            request(options, function (err, resp, body) {
                if (!err && resp.statusCode === 200) {
                    resp.setEncoding('utf8');
                    var resultAsJSON = JSON.parse(body); 
                    item.successed = true;
                    subcallback(null, resultAsJSON);
                }else{
                    item.successed = false;
                    printLog('  ArtistsURL: "' + artistsURL + '" Error: ' + err + ': ' + utility.filterLineBreak(body));
                    subcallback(true, '');
                }
            });
        },
        function(json, subcallback){
            // console.log(json.images);
            appendImagesToArtists(json.artists, true);
            subcallback();
        }
    ], function (err, result) {
        if(g_data.retryLevel == 0)
            g_data.currentSpotifyPos += count;
        callback();
    });
}

function appendImagesToArtists(artists, saveCache){
    if(!artists || artists.length == 0)
        return;
    for(var i = 0; i< artists.length; i++){
        if(artists[i]){
            var id = artists[i].id;
            var index = g_data.musicStreamingArrayIndex[id];
            if(index !== undefined){
                var obj = g_data.musicStreamingArray[index];
                obj.images = artists[i].images;
                if(saveCache){
                    var cached = {
                        id: id,
                        name: obj.name,
                        application: 'Spotify',
                        href: obj.href,
                        images: obj.images
                    }
                    g_data.artists_newCacheArray[id] = cached;
                }                
            }

        }
    }
}

// ===== STEP 4: Post result =====
function printResult(callback){
    g_data.likesArrayCompacted = getCompactedArray(g_data.likesArray);
    g_data.musicStreamingArrayCompacted = getCompactedArray(g_data.musicStreamingArray);
    printLog(' Total Customer Fetched: ' + g_data.totalCustomerCount);
    printLog(' - Success: ' + g_data.fetchSuccessed.length + ', male: ' + g_data.maleCount + ', female: ' + g_data.femaleCount);
    printLog(' - Not Found: ' + g_data.fetchNotFound.length + ', ids: ' + g_data.fetchNotFound.toString());
    printLog(' - Failed: ' + g_data.fetchFailed.length + ', ids: ' + g_data.fetchFailed.toString());
    printLog(' Total Likes Fetched: ' + g_data.totalLikesLoaded + ', Unique Count: ' + g_data.likesArray.length + ', Compacted Count: ' + g_data.likesArrayCompacted.length);
    printLog(' Total Tracks Fetched: ' + g_data.totalTracksLoaded + ', Unique Count: ' + g_data.tracksArray.length);    
    printLog(' - Success: ' + g_data.fetchArtistSuccessed.length);
    printLog(' - Failed: ' + g_data.fetchArtistFailed.length + ', ids: ' + g_data.fetchArtistFailed.toString());
    printLog(' Total Artists Fetched: ' + g_data.fetchArtistSuccessed.length + ', Unique Count: ' + g_data.artistsArray.length);
    printLog(' Total MusicListens Fetched: ' + g_data.artistsArray.length + ', Filtered Count: ' + g_data.musicStreamingArray.length + ', Compacted Count: ' + g_data.musicStreamingArrayCompacted.length);
    printLog(' Music Apps: ' + JSON.stringify(g_data.musicApps));
    callback();
}

function getCompactedArray(srcArray){
    var compactedArray = [];
    for(var i=0;i<srcArray.length;i++){
        var obj = srcArray[i];
        if(obj && obj.count >= CONSTANTS.compactThreshold){
            compactedArray.push(obj);
        }
    }
    return compactedArray;
}

// ===== STEP 5: Cache API call =====
function cacheToJSON(callback){
    var t = process.hrtime();
    cacheData(CONSTANTS.cacheType_Track, function(){
        cacheData(CONSTANTS.cacheType_Artist, function(){
            g_data.time_step5 = process.hrtime(t)[0];
            printLog(' Time Elapsed: ' + g_data.time_step5 + '(s)');
            callback();
        });
    });
}

function cacheData(type, callback){
    var basePath = type == CONSTANTS.cacheType_Track ? CONSTANTS.cacheTrack : CONSTANTS.cacheArtist;
    var pathIndexFile = basePath + CONSTANTS.cacheIndexFile;
    var newCache = type == CONSTANTS.cacheType_Track ? g_data.tracks_newCacheArray : g_data.artists_newCacheArray;
    var loadedCache = type == CONSTANTS.cacheType_Track ? g_data.tracks_loadedCacheArray : g_data.artists_loadedCacheArray;    

    var loadedCount = Object.keys(loadedCache.data).length;
    var newCacheCount = Object.keys(newCache).length;
    var totalCount = loadedCount + newCacheCount;

    var outputIndex = loadedCache;
    outputIndex.count = totalCount;
    var startNum = loadedCount + 1;

    var folderPath = '';
    for (var key in newCache) {
        var obj = newCache[key];

        var newFolderPath = utility.strPad(Math.ceil(startNum++ / CONSTANTS.cacheFilesInFolder), '0000');
        if(folderPath != newFolderPath){
            folderPath = newFolderPath;
            utility.createDirectory(basePath + folderPath, printLog);
        }
        var filePath = basePath + folderPath + '/' + key + '.json';

        jsonfile.writeFileSync(filePath, obj);
        outputIndex.data[key] = folderPath;
    }
    jsonfile.writeFile(pathIndexFile, outputIndex, function (err) {
      printLog(' Caching ' + type + ' Count: ' + totalCount + '(' + newCacheCount +' newly cached), Path:' + pathIndexFile + ', Result:' + (err ? err : 'successed.'));
      callback();
    });
}

// ===== STEP 6: Post compacted(filtered by threshold) to DB =====
function prepareJSON(compacted){
    var ret = {
        _id:    g_args.type + '-' + g_args.id + '-' + g_args.section + (compacted ? '-compact' : ''),
        type:   g_args.type,
        data:   {}
    };
    
    if(g_args.includeIDs == "true")
        ret.data.customerIDs = g_data.customerIDArray;
    if(g_args.section == SECTION_ALL || g_args.section == SECTION_GENDER)
        ret.data.gender = { male: g_data.maleCount, female: g_data.femaleCount };
    if(g_args.section == SECTION_ALL || g_args.section == SECTION_LIKES){
        ret.data.likes = compacted ? g_data.likesArrayCompacted : g_data.likesArray;
    }
    if(g_args.section == SECTION_ALL || g_args.section == SECTION_MUSIC){
        ret.data.musicstreaming = compacted ? g_data.musicStreamingArrayCompacted : g_data.musicStreamingArray;
    }
    return ret;
}

function postCompactedToDB(callback){
    var t = process.hrtime();

    var postJSON = prepareJSON(true);
    // ----- check existance
    var srcURL = CONSTANTS.postURL + '_all_docs?keys=["' +  postJSON._id+'"]';
    var options = {
        url: srcURL,
        timeout: CONSTANTS.timeOut,
        headers:{
            'Authorization': CONSTANTS.auth
        }
    };
    printLog(' Check if db exists already ' + srcURL);
    
    request(options, function (err, resp, body) {
        if (!err && resp.statusCode === 200) {
            var resultAsJSON = JSON.parse(body);
            var rev = resultAsJSON.rows[0].value;
            if(rev){
                postJSON._rev = rev.rev;
                printLog(' - Found document: ' + postJSON._id + ', rev: ' + postJSON._rev + ', overwriting...');
            }else{
                printLog(' - Not found document: ' + postJSON._id + ', creating new...');
            }            
        }else{
            printLog(' - HTTP Request error: ' + postJSON._id + ', creating new...');
            //printLog(" - Response Body: " + JSON.stringify(body));            
        }
        printLog(' - To: ' + CONSTANTS.postURL + postJSON._id);

        var options = {
            url:    CONSTANTS.postURL,
            method: 'POST',
            json:   postJSON,
            headers:{
                'Authorization': CONSTANTS.auth,
                "content-type": "application/json"
            }
        };
        request(options, function (err, resp, body) {
            g_data.time_step6 = process.hrtime(t)[0];
            printLog(' Time Elapsed: ' + g_data.time_step6 + '(s)');
            if (!err && (resp.statusCode === 200 || resp.statusCode === 201)) {
                callback();
            }else{
                callback(true, 'STEP 6 - Posting failure : ' + err);
            }            
        });
    });    
}

// ===== STEP 7: Post to DB =====
function postToDB(callback){
    var t = process.hrtime();

    var postJSON = prepareJSON(false);
    // ----- check existance
    var srcURL = CONSTANTS.postURL + '_all_docs?keys=["' +  postJSON._id+'"]';
    var options = {
        url: srcURL,
        timeout: CONSTANTS.timeOut,
        headers:{
            'Authorization': CONSTANTS.auth
        }
    };
    printLog(' Check if db exists already ' + srcURL);
    
    request(options, function (err, resp, body) {
        if (!err && resp.statusCode === 200) {
            var resultAsJSON = JSON.parse(body);
            var rev = resultAsJSON.rows[0].value;
            if(rev){
                postJSON._rev = rev.rev;
                printLog(' - Found document: ' + postJSON._id + ', rev: ' + postJSON._rev + ', overwriting...');
            }else{
                printLog(' - Not found document: ' + postJSON._id + ', creating new...');
            }            
        }else{
            printLog(' - HTTP Request error: ' + postJSON._id + ', creating new...');
            //printLog(" - Response Body: " + JSON.stringify(body));            
        }
        printLog(' - To: ' + CONSTANTS.postURL + postJSON._id);

        var options = {
            url:    CONSTANTS.postURL,
            method: 'POST',
            json:   postJSON,
            headers:{
                'Authorization': CONSTANTS.auth,
                "content-type": "application/json"
            }
        };
        request(options, function (err, resp, body) {
            g_data.time_step7 = process.hrtime(t)[0];
            printLog(' Time Elapsed: ' + g_data.time_step7 + '(s)');
            if (!err && (resp.statusCode === 200 || resp.statusCode === 201)) {
                callback();
            }else{
                callback(true, 'STEP 7 - Posting failure : ' + err);
            }            
        });
    });    
}
