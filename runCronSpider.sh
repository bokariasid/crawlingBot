#!/bin/bash

cd /home/ro/naukriJobCrawl/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl naukriFormSubmit
