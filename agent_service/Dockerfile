ARG BASE_REGISTRY=docker.io/library
FROM ${BASE_REGISTRY}/node:20-slim

WORKDIR /opt/agent
COPY agent_service/package.json /opt/agent/package.json
RUN npm install --omit=dev

COPY agent_service/server.js /opt/agent/server.js

EXPOSE 8098
ENV AGENT_PORT=8098
CMD ["node", "server.js"]


