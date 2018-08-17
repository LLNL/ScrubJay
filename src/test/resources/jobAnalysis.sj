{
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
      "originalScrubJaySchema": {
        "columns": [
          {
            "name" : "jobid",
            "domain" : true,
            "dimension" : {
              "name": "job",
              "ordered": false,
              "continuous": false,
              "subDimensions": []
            },
            "units" : {
              "name" : "identifier",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "nodelist",
            "dimension" : {
              "name": "node",
              "ordered": false,
              "continuous": false,
              "subDimensions": []
            },
            "domain" : true,
            "units" : {
              "name": "list",
              "elementType": "MULTIPOINT",
              "subUnits": {
                "listUnits": {
                  "name" : "identifier",
                  "elementType" : "POINT"
                }
              }
            }
          },
          {
            "name" : "elapsed",
            "dimension" : {
              "name": "time",
              "ordered": true,
              "continuous": true,
              "subDimensions": []
            },
            "domain" : false,
            "units" : {
              "name" : "seconds",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "timespan",
            "dimension" : {
              "name": "time",
              "ordered": true,
              "continuous": true,
              "subDimensions": []
            },
            "domain" : true,
            "units" : {
              "name" : "range",
              "elementType" : "RANGE",
              "subUnits" : {
                "rangeUnits" : {
                  "name" : "datetimestamp",
                  "elementType" : "POINT"
                }
              }
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
      "originalScrubJaySchema": {
        "columns": [
          {
            "name" : "node",
            "domain" : true,
            "dimension" : {
              "name": "node",
              "ordered": false,
              "continuous": false,
              "subDimensions": []
            },
            "units" : {
              "name" : "identifier",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "rack",
            "domain" : true,
            "dimension" : {
              "name": "rack",
              "ordered": false,
              "continuous": false,
              "subDimensions": []
            },
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
      "originalScrubJaySchema": {
        "columns": [
          {
            "name" : "node",
            "domain" : true,
            "dimension" : {
              "name": "node",
              "ordered": false,
              "continuous": false,
              "subDimensions": []
            },
            "units" : {
              "name" : "identifier",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "time",
            "domain" : true,
            "dimension" : {
              "name": "time",
              "ordered": true,
              "continuous": true,
              "subDimensions": []
            },
            "units" : {
              "name" : "datetimestamp",
              "elementType" : "POINT"
            }
          },
          {
            "name" : "flops",
            "domain" : false,
            "dimension" : {
              "name": "flops",
              "ordered": true,
              "continuous": true,
              "subDimensions": []
            },
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
