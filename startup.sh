#!/bin/bash

if   [  $SERVER_PORT  ];
then
   port=$SERVER_PORT chain &
else
   port=8000 chain &
fi

tail -f startup.sh