FROM node:22-bookworm-slim

WORKDIR /app
COPY whatsapp-bridge/baileys-gateway/package.json whatsapp-bridge/baileys-gateway/package-lock.json ./
RUN npm ci --omit=dev
COPY whatsapp-bridge/baileys-gateway/delivery-store.js whatsapp-bridge/baileys-gateway/index.js ./

CMD ["node", "index.js"]
