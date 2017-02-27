var express = require('express');
var router = express.Router();
var request = require('request');

/* GET users listing. */
router.post('/total', function(req, res, next) {
    var response = {};

    if(typeof req.body.json === 'undefined')
	return res.send('{"error":"missing parameter json"}');

    var elasticsearchReqParams = JSON.parse(req.body.json);

    var elasticsearchReq = {
			  "query": {
			    "bool": {
			    }
			  },
			  "fields": ["id",
				     "value.Krankenhaus_Name",
				     "value.Krankenhaus_Traeger_Art",
				     "value.location.lat",
				     "value.location.lng",
				     "value.Bundesland",
				     "value.Land",
				     "value.Hausanschrift_Ort",
				     "value.Hausanschrift_Postleitzahl",
				     "value.Hausanschrift_Strasse",
				     "value.Hausanschrift_Hausnummer",
				     "value.Anzahl_Betten"
				    ]
			};
    var must = new Array();
    must.push({"match_phrase": {
				"Behandlungen_Und_Diagnosen": elasticsearchReqParams.opsIcd
	      }});

    if(typeof elasticsearchReqParams.Krankenhaus_Name !== "undefined" &&  elasticsearchReqParams.Krankenhaus_Name.length !== 0)
    {
	must.push({"match": {"Krankenhaus_Name" : elasticsearchReqParams.Krankenhaus_Name}});
    }

    if(typeof elasticsearchReqParams.Hausanschrift_Ort !== "undefined" &&  elasticsearchReqParams.Hausanschrift_Ort.length !== 0)
    {
	must.push({"match": {"Hausanschrift_Ort" : elasticsearchReqParams.Hausanschrift_Ort}});
    }

    if(typeof elasticsearchReqParams.Bundesland !== "undefined" &&  elasticsearchReqParams.Bundesland.length !== 0)
    {
	var qElements = "";
	for(var element in elasticsearchReqParams.Bundesland)
	{
		qElements = qElements + elasticsearchReqParams.Bundesland[element] + " ";
	}
	must.push({"match": {"Bundesland" : {"query":qElements, "operator": "or"}}});
    }

    if(typeof elasticsearchReqParams.Krankenhaus_Traeger_Art !== "undefined" &&  elasticsearchReqParams.Krankenhaus_Traeger_Art.length !== 0)
    {
	var qElements = "";
	for(var element in elasticsearchReqParams.Krankenhaus_Traeger_Art)
	{
		qElements = qElements + elasticsearchReqParams.Krankenhaus_Traeger_Art[element] + " ";
	}
	must.push({"match": {"Krankenhaus_Traeger_Art" : {"query":qElements, "operator": "or"}}});
    }

    elasticsearchReq.query.bool.must = must;

    console.log("POST:");
    console.log(JSON.stringify(elasticsearchReq));

	var options = {
	  uri: 'http://127.0.0.1:9200/krankenhauser/_search?size=10000',
	  method: 'POST',
	  json: elasticsearchReq
	};

    request.post(
	    options,
	    function (error, response, body) {

		if (!error && typeof body !== "undefined" && typeof body.hits !== "undefined" && typeof body.hits.hits !== "undefined") {
		    console.log("body:");
		    console.log(body.hits.hits);

		    var opsIcdCode = elasticsearchReqParams.opsIcd;
		    var years = ['06','08','10','12','13'];
		    var total = {};
		    var krankenhauserTotal = new Array();
		    var apiResponse = {};
		    for(var i in body.hits.hits)
		    {
			//var krankenhausId = $IDS[body.hits.hits[i]._id];
			var krankenhausTotal = {};
			var krankenhausId = body.hits.hits[i].fields.id;
			var krankenhausInfos = body.hits.hits[i].fields;
			/* CLEANING */
			if(typeof krankenhausInfos["value.Krankenhaus_Name"] !== "undefined")
				krankenhausInfos["Krankenhaus_Name"] = krankenhausInfos["value.Krankenhaus_Name"][0];
			if(typeof krankenhausInfos["value.Krankenhaus_Traeger_Art"] !== "undefined")
				krankenhausInfos["Krankenhaus_Traeger_Art"] = krankenhausInfos["value.Krankenhaus_Traeger_Art"][0];
			if(typeof krankenhausInfos["value.location.lat"] !== "undefined")
				krankenhausInfos["locationLat"] = krankenhausInfos["value.location.lat"][0];
			if(typeof krankenhausInfos["value.location.lng"] !== "undefined")
				krankenhausInfos["locationLng"] = krankenhausInfos["value.location.lng"][0];
			if(typeof krankenhausInfos["value.Bundesland"] !== "undefined")
				krankenhausInfos["Bundesland"] = krankenhausInfos["value.Bundesland"][0];
			if(typeof krankenhausInfos["value.Land"] !== "undefined")
				krankenhausInfos["Land"] = krankenhausInfos["value.Land"][0];
			if(typeof krankenhausInfos["value.Hausanschrift_Ort"] !== "undefined")
				krankenhausInfos["Hausanschrift_Ort"] = krankenhausInfos["value.Hausanschrift_Ort"][0];
			if(typeof krankenhausInfos["value.Hausanschrift_Postleitzahl"] !== "undefined")
				krankenhausInfos["Hausanschrift_Postleitzahl"] = krankenhausInfos["value.Hausanschrift_Postleitzahl"][0];
			if(typeof krankenhausInfos["value.Hausanschrift_Strasse"] !== "undefined")
				krankenhausInfos["Hausanschrift_Strasse"] = krankenhausInfos["value.Hausanschrift_Strasse"][0];
			if(typeof krankenhausInfos["value.Hausanschrift_Hausnummer"] !== "undefined")
				krankenhausInfos["Hausanschrift_Hausnummer"] = krankenhausInfos["value.Hausanschrift_Hausnummer"][0];
			if(typeof krankenhausInfos["value.Anzahl_Betten"] !== "undefined")
				krankenhausInfos["Anzahl_Betten"] = krankenhausInfos["value.Anzahl_Betten"][0];

			/* END OF CLEANING*/

			krankenhausTotal = krankenhausInfos;
			krankenhausTotal['ik'] = body.hits.hits[i]._id;
			krankenhausTotal['id'] = krankenhausId;
			krankenhausTotal['sum'] = {};
			for(var year in years)
				    {
					if(typeof $KV[opsIcdCode] !== 'undefined' && typeof $KV[opsIcdCode][years[year]]!== 'undefined')
					{
						if(typeof total['20'+years[year]] === "undefined")
						     total['20'+years[year]] = 0;
						if(typeof $KV[opsIcdCode][years[year]][krankenhausId] !== "undefined")
						     total['20'+years[year]] += $KV[opsIcdCode][years[year]][krankenhausId];

						if(typeof $KV[opsIcdCode][years[year]][krankenhausId] !== "undefined")
							krankenhausTotal['sum']['20'+years[year]] = Math.round($KV[opsIcdCode][years[year]][krankenhausId]);
						else
							krankenhausTotal['sum']['20'+years[year]] = 0;
						
						/*
						console.log("ID:");
						console.log($IDS[body.hits.hits[i]._id]);
						console.log("count:");
						console.log($KV[opsIcdCode][years[year]][$IDS[body.hits.hits[i]._id]]);
						console.log("opsIcdCode:");
						console.log(opsIcdCode);*/
						//console.log($KV[opsIcdCode][years[year]]);
					}
					else
					{
						console.log("no data for year: " + years[year] + "");
					}
			}
			krankenhauserTotal.push(krankenhausTotal);
		    }

		    for(var year in total)
		    {
			total[year] = Math.round(total[year]);
		    }
		    apiResponse['krankenhauserTotal'] = krankenhauserTotal;
		    apiResponse['total'] = total;

		    res.send(JSON.stringify(apiResponse));
		}
		else
		{
		    console.log("error:");
		    console.log(error);
		}
	    }
    );

    console.log("search for: " + req.body.json);

});

module.exports = router;
