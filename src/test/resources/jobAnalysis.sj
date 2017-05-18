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
            "dimension": "job",
            "domain": true
          },
          {
            "name" : "nodelist",
            "dimension": "node",
            "domain": true
          },
          {
            "name" : "elapsed",
            "dimension": "time",
            "domain": false
          },
          {
            "name" : "timespan",
            "dimension": "time",
            "domain": true
          }
        ]
      }
    }
  ]
}
