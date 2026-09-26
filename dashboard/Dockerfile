FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY operator-dashboard.js ./
EXPOSE 3000
CMD ["node", "operator-dashboard.js"]
