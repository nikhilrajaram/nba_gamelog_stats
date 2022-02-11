#!/bin/bash
rm -rf data/gamelogs
mkdir -p .tmp && cd .tmp
git clone https://github.com/nikhilrajaram/bbref-scrape .
go build cmd/bbref-scrape/scrape.go
mkdir -p output/gamelogs
./scrape 2022
cd ..
mkdir -p data && mv .tmp/output/gamelogs data
rm -rf .tmp
