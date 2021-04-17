#!/bin/bash
sudo mysql -uroot -pmoves --force < CreateMOVESUser.sql
sudo mysql -uroot -pmoves < movesdb20210209.sql
