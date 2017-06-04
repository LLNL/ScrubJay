{
  "dimensionSpace" : {
    "dimensions": [
      {
        "name": "time",
        "ordered": true,
        "continuous": true
      },
      {
        "name": "job",
        "ordered": false,
        "continuous": false
      },
      {
        "name": "rack",
        "ordered": false,
        "continuous": false
      },
      {
        "name": "flops",
        "ordered": true,
        "continuous": true
      },
      {
        "name": "node",
        "ordered": false,
        "continuous": false
      }
    ] },
  "datasets": [
    {
      "type" : "CSVDatasetID",
      "csvFileName" : "target/scala-2.11/test-classes/jobQueue.csv",
      "options" : {
        "header" : "true",
        "delimiter" : "|"
      },
      "sparkSchema" : {
        "type" : "struct",
        "fields" : [ {
          "name" : "jobid",
          "type" : "string",
          "nullable" : true,
          "metadata" : { }
        }, {
          "name" : "nodelist",
          "type" : "string",
          "nullable" : true,
          "metadata" : {
            "scrubJayType" : {
              "type" : "ArrayString",
              "delimiter" : ","
            }
          }
        }, {
          "name" : "elapsed",
          "type" : "integer",
          "nullable" : true,
          "metadata" : {
            "scrubJayType_UNIMPLEMENTED" : {
              "type" : "Seconds"
            }
          }
        }, {
          "name" : "timespan",
          "type" : "string",
          "nullable" : true,
          "metadata" : {
            "scrubJayType" : {
              "type" : "LocalDateTimeRangeString",
              "dateformat" : "yyyy-MM-dd'T'HH:mm:ss"
            }
          }
        } ]
      },
      "scrubJaySchema": {
        "fields": [
          {
            "name" : "jobid",
            "domain" : true,
            "dimension" : "job",
            "units" : {
              "name" : "identifier",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "nodelist",
            "dimension" : "node",
            "domain" : true,
            "units" : {
              "name" : "list<identifier>",
              "elementType" : "MULTIPOINT"
            }
          },
          {
            "name" : "elapsed",
            "dimension" : "time",
            "domain" : false,
            "units" : {
              "name" : "seconds",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "timespan",
            "dimension" : "time",
            "domain" : true,
            "units" : {
              "name" : "datetimespan",
              "elementType" : "RANGE"
            }
          }
        ]
      }
    },
    {
      "type": "CSVDatasetID",
      "csvFileName": "target/scala-2.11/test-classes/clusterLayout.csv",
      "options": {
        "header": "true",
        "delimiter": ","
      },
      "sparkSchema": {
        "type": "struct",
        "fields": [
          {
            "name": "node",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "rack",
            "type": "string",
            "nullable": true,
            "metadata": {}
          }
        ]
      },
      "scrubJaySchema": {
        "fields": [
          {
            "name" : "node",
            "domain" : true,
            "dimension" : "node",
            "units" : {
              "name" : "identifier",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "rack",
            "domain" : true,
            "dimension" : "rack",
            "units" : {
              "name" : "identifier",
              "elementType" : "POINT"
            }
          }
        ]
      }
    },
    {
      "type": "CSVDatasetID",
      "csvFileName": "target/scala-2.11/test-classes/nodeFlops.csv",
      "options": {
        "header": "true",
        "delimiter": ","
      },
      "sparkSchema": {
        "type": "struct",
        "fields": [
          {
            "name": "node",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "time",
            "type": "string",
            "nullable": true,
            "metadata": {
              "scrubJayType": {
                "type": "LocalDateTimeString",
                "dateformat": "yyyy-MM-dd'T'HH:mm:ss"
              }
            }
          },
          {
            "name": "flops",
            "type": "integer",
            "nullable": true,
            "metadata": {}
          }
        ]
      },
      "scrubJaySchema": {
        "fields": [
          {
            "name" : "node",
            "domain" : true,
            "dimension" : "node",
            "units" : {
              "name" : "identifier",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "time",
            "domain" : true,
            "dimension" : "time",
            "units" : {
              "name" : "datetimestamp",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "flops",
            "domain" : false,
            "dimension" : "flops",
            "units" : {
              "name" : "count",
              "elementType" : "POINT"
            }
          }
        ]
      }
    }
  ]
}
