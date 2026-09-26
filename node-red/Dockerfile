FROM nodered/node-red:4.0
# Official MongoDB driver for the alert-store function node (shared client via functionGlobalContext).
RUN npm install --no-fund --no-audit mongodb@6
COPY settings.js flows.json /usr/src/node-red/
ENTRYPOINT ["node", "node_modules/node-red/red.js", "--userDir", "/data", "--settings", "/usr/src/node-red/settings.js"]
