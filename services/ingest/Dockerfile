FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY telemetry-ingest.js ./
EXPOSE 3001
CMD ["node", "telemetry-ingest.js"]
