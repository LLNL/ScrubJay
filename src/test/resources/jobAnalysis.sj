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
        "name": "node",
        "ordered": false,
        "continuous": false
      }
    ] },
  "datasets": [
    {
      "type": "CSVDatasetID",
      "csvFileName": "target/scala-2.11/test-classes/jobQueue.csv",
      "options": {
        "header": "true",
        "delimiter": "|"
      },
      "sparkSchema": {
        "type": "struct",
        "fields": [
          {
            "name": "jobid",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "nodelist",
            "type": "string",
            "nullable": true,
            "metadata": {
              "scrubjay_parser": {
                "type": "ArrayString",
                "delimiter": ","
              }
            }
          },
          {
            "name": "elapsed",
            "type": "integer",
            "nullable": true,
            "metadata": {
              "scrubjay_parser_UNIMPLEMENTED": {
                "type": "Seconds"
              }
            }
          },
          {
            "name": "timespan",
            "type": "string",
            "nullable": true,
            "metadata": {
              "scrubjay_parser": {
                "type": "LocalDateTimeRangeString",
                "dateformat": "yyyy-MM-dd'T'HH:mm:ss"
              }
            }
          }
        ]
      },
      "scrubJaySchema": {
        "fields": [
          {
            "name" : "jobid",
            "dimension" : "job",
            "units" : "identifier",
            "domain" : true
          },
          {
            "name" : "nodelist",
            "dimension" : "node",
            "units" : "list<identifier>",
            "domain" : true
          },
          {
            "name" : "elapsed",
            "dimension" : "time",
            "units" : "seconds",
            "domain" : false
          },
          {
            "name" : "timespan",
            "dimension" : "time",
            "units" : "datetimespan",
            "domain" : true
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
            "dimension" : "node",
            "units" : "identifier",
            "domain" : true
          },
          {
            "name" : "rack",
            "dimension" : "rack",
            "units" : "identifier",
            "domain" : true
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
              "scrubjay_parser": {
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
            "dimension" : "node",
            "units" : "identifier",
            "domain" : true
          },
          {
            "name" : "time",
            "dimension" : "time",
            "units" : "datetimestamp",
            "domain" : true
          },
          {
            "name" : "flops",
            "dimension" : "flops",
            "units" : "int",
            "domain" : false
          }
        ]
      }
    }
  ]
}
