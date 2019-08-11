# Pelican

## Problem

You have an old database that needs to be extracted and ingested into a data lake, but you don't want to keep it around, maintain servers, etc.

## Overview

Pelican provides a configurable way to wrap a SQL dump in a Docker image based on the database type specified, load the data into the database in the Docker container and then begin and extraction process that dumps the data into a Kafka queue.
