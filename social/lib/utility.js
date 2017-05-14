var fs = require('fs');
var nodefs = require('node-fs');
var jsonfile= require('jsonfile');

module.exports = {
	filterLineBreak: function (str, instead){
		if (typeof str === 'undefined') {
			return '';
		}
		if (typeof instead === 'undefined') {
			instead = '';
		}
	    var ret = str.replace(/\r\n/g, instead);
	    return ret.replace(/\n/g, instead);
	},

	filterSpecialCharsForCSV: function (str){
	    var ret = str.replace(/\"/g, "'");
	    return this.filterLineBreak(ret, ' ');
	},

	getCustomerIDsFromFile: function(path){
		try {
			var str = fs.readFileSync(path).toString();
			str = this.filterLineBreak(str,',');
	    	return str.split(',');
		} catch (e) {
		    console.log('Error:', e);
		    return [];
		}
	},

	strPad: function(number, pad){
		var str = "" + number;
		return pad.substring(0, pad.length - str.length) + str;
	},

	prepareFileOutput: function (folder, filename){
		var dir = './' + folder;
	    if (!fs.existsSync(dir)){
	        fs.mkdirSync(dir);
	    }
	    var path = folder + '/' + filename;
	    return path;
	},

	createDirectory: function(folder, logFunc){
		nodefs.mkdirSync(folder, 0777, true);
		// logFunc('Directory ' + folder + ' created.');
	},

	loadJSONtoObject: function(path){
		try{
			return jsonfile.readFileSync(path);
		} catch (err) {
			return null;
		}
	},
	
	checkDirectorySync: function (directory) {  
		try {
			fs.statSync(directory);
		} catch(e) {
			fs.mkdirSync(directory);
		}
	}
}