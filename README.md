# ![IKATS Logo](https://ikats.github.io/img/Logo-ikats-icon.png) IKATS Datamodel

![Docker Automated build](https://img.shields.io/docker/automated/ikats/datamodel.svg)
![Docker Build Status](https://img.shields.io/docker/build/ikats/datamodel.svg)
![MicroBadger Size](https://img.shields.io/microbadger/image-size/ikats/datamodel.svg)

**An overview of IKATS global architecture is available [here](https://github.com/IKATS/IKATS)**

IKATS datamodel provides two IKATS web applications :
* the **Ingestion** webapp, deployed in a Tommee server to ingest data in IKATS : cf.[ingestion documentation](https://github.com/IKATS/ikats-datamodel/tree/master/ikats-ingestion)
* the **TemporalDataManager** webapp, deployed in a Tomcat server, which is describer here.

The **TemporalDataManager** webapp provides access to following IKATS resources:

On PostgreSQL database:

* Metadata
* Dataset (set of time series)
* Table
* MacroOperator
* ProcessData
* Workflow

On OpenTSDB database:

* Time Series

Resources can be accessed through an HTTP API, including IKATS operators dealing with non temporal data.

List of java operators at the moment: (see [python operators](https://github.com/IKATS?q=op-) for other operators provided in IKATS)

## Dataset Preparation

### Import Export

* [Import TS](https://ikats.org/doc/operators/importTs.html)
* [Import Metadata](https://ikats.org/doc/operators/importMetadata.html)

### Dataset Management

* [Dataset Selection](https://ikats.org/doc/operators/datasetSelection.html)
* [Manual Selection](https://ikats.org/doc/operators/manualSelection.html)
* [TS Finder](https://ikats.org/doc/operators/tsFinder.html)
* [Filter](https://ikats.org/doc/operators/filter.html)
* [Merge TS lists](https://ikats.org/doc/operators/mergeTsLists.html)
* [Save as a Dataset](https://ikats.org/doc/operators/saveAsDataset.html)

## Pre-Processing on Ts

### Transforming

* [Ts2Feature](https://ikats.org/doc/operators/ts2Feature.html)
* [Discretize](https://ikats.org/doc/operators/discretize.html)

## Processing On Tables

* [Read Table](https://ikats.org/doc/operators/readTable.html)
* [TrainTestSplit](https://ikats.org/doc/operators/trainTestSplit.html)
* [Merge Tables](https://ikats.org/doc/operators/mergeTables.html)
* [Population Selection](https://ikats.org/doc/operators/populationSelection.html)
