FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY booking-dispatch.js ./
EXPOSE 3002
CMD ["node", "booking-dispatch.js"]
