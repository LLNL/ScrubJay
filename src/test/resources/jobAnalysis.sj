{
  "dimensions" : [ {
    "name" : "time",
    "ordered" : true,
    "continuous" : true
  }, {
    "name" : "job",
    "ordered" : false,
    "continuous" : false
  }, {
    "name" : "node",
    "ordered" : false,
    "continuous" : false
  } ],
  "datasets" : [ {
    "type" : "CSVDatasetID",
    "csvFileName" : "target/scala-2.11/test-classes/jobQueue.csv",
    "schema" : {
      "type" : "struct",
      "fields" : [ {
        "name" : "jobid",
        "type" : "string",
        "nullable" : true,
        "metadata" : {
          "domain" : true,
          "units" : "identifier",
          "dimension" : "job"
        }
      }, {
        "name" : "nodelist",
        "type" : "string",
        "nullable" : true,
        "metadata" : {
          "scrubjaytype" : "ArrayString",
          "subtype" : {
            "type" : "string"
          },
          "domain" : true,
          "dimension" : "node",
          "delimiter" : ","
        }
      }, {
        "name" : "elapsed",
        "type" : "integer",
        "nullable" : true,
        "metadata" : {
          "domain" : false,
          "units" : "seconds",
          "dimension" : "time"
        }
      }, {
        "name" : "timespan",
        "type" : "string",
        "nullable" : true,
        "metadata" : {
          "domain" : true,
          "scrubjaytype" : "LocalDateTimeRangeString",
          "dateformat" : "yyyy-MM-dd'T'HH:mm:ss",
          "dimension" : "time"
        }
      } ]
    },
    "options" : {
      "header" : "true",
      "delimiter" : "|"
    },
    "isValid" : true
  } ]
}
