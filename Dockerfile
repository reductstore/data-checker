FROM node:18-slim

COPY package.json .
COPY package-lock.json .

RUN npm ci

COPY . .

CMD ["npm", "start"]
