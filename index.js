"use strict";

var fs = require('fs');
var parse = require('csv-parse');
var async = require('async');
var path = require('path');
var _ = require('underscore');

var csv = function (name) {
	return new Promise(function (resolve, reject) {
		var file = path.join(__dirname, name);

		var parser = parse({delimiter: ','}, function (err, data) {
			if(err){
				console.log('parser err', err);
				reject(err);
			}
			data.shift(); //remove header
			resolve(data);
		})
		fs.createReadStream(file).pipe(parser);		
	});
};

var rate = function (opened, sends) {
	var o =  opened / sends * 100;
	var openRate = Math.round(o * 100) / 100;
	return openRate;
};

var getStats = function (camp, field1, separator, field2) {
	//TODO: I should refactor this to honor the DRY
	var stats = {};
	_(camp).each(function (value, key) {
		if(
			!value.user || !value.stats || 
			(value.stats[2] !== 'Yes' && value.stats[2] !== 'No') || 
			(value.stats[3] !== 'Yes' && value.stats[3] !== 'No')
		){
			throw new Error('Data set error for :' + key + JSON.stringify(value,2,2))
		}
		var occupation = value.user[2] || '';
		var messageSent = value.user[3] || '';
		var opened = (value.stats[2] === 'Yes');
		var clicked = (value.stats[3] === 'Yes');

		var ops = {
			occupation: occupation,
			messageSent: messageSent,
			opened: opened,
			clicked: clicked
		};

		var k = (ops[field1] || '') + separator + (ops[field2] || '');

		if(!stats[k]){
			stats[k] = {
				opened: 0,
				clicked: 0,
				sends: 0,
				openRate: 0,
				clickRate: 0
			}
		}

		if(opened){
			stats[k].opened++;
		}

		if(clicked){
			stats[k].clicked++;
		}

		stats[k].sends++;
		stats[k].openRate = rate(stats[k].opened, stats[k].sends); //that's ok to recalculate for each iteration
		stats[k].clickRate = rate(stats[k].clicked, stats[k].sends); //that's ok to recalculate for each iteration

	});

	console.log('Printed in format to copy/paste on excel:')
	_(stats).each(function (value, key) {
		console.log(key + '\t' + value.opened + '\t' + value.clicked + '\t' + value.sends + '\t' + value.openRate/100 + '\t' + value.clickRate/100);
	});

	_(stats).each(function (value, key) {
		console.log(key + ':');
		console.log('	Opens: ', value.opened);
		console.log('	Clicks: ', value.clicked);
		console.log('	Sends: ', value.sends);
		console.log('	Open rate: ', value.openRate, '%');
		console.log('	Click rate: ', value.clickRate, '%');
	});
		
};

var preCampaign, responseRate;
Promise.resolve()
.then(function () {
	return csv('data/pre_campaign.csv');
})
.then(function (data) {
	preCampaign = data;
})
.then(function () {
	return csv('data/response_rate.csv');	
})
.then(function (data) {
	responseRate = data;
})
.then(function () {

	var camp = {};
	_(preCampaign).each(function (line) {
		var name = line[0];
		camp[name] = {
			stats: null,
			user: line,
			count: 0
		};
	});

	_(responseRate).each(function (line) {
		var name = line[0] + ' ' + line[1];
		var opened = line[3];
		if( _(camp).has(name) ){
			if(opened === 'Yes'){
				camp[name].count++;
			}
			camp[name].stats = line;
		}
	});


	//error checking	
	_(camp).each(function (value, key) {
		if(value.count !== 0 && value.count !== 1){
			throw new Error('There is not one to one relationship on the datasets. Error with ' + key + ': ' + value);
		}
	});

	//at this point we are safe to assume that there are 
	//one 'Opened' information for each user of the pre-campaign
	//so our data is good

	var opened = 0;
	var sends = 0;
	_(camp).each(function (value, key) {
		if(value.count === 1){
			opened++;
		}
		sends++;
	});
	var openRate = rate(opened, sends);
	console.log('Opens: ' + opened);
	console.log('Sends: ' + sends);
	console.log('Open rate: ' + openRate + '%');

	getStats(camp, 'occupation', 	' role with ', 'messageSent'	);
	getStats(camp, '', 				''		, 'messageSent'	);
	getStats(camp, 'occupation', 	''		, ''			);

})
.catch(function (err) {
	console.log('error', err);
});

