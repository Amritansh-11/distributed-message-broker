FROM node:20-alpine

# Create app directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install production dependencies
RUN npm ci --only=production || npm install --only=production

# Copy application source code
COPY . .

# Expose default TCP broker port and HTTP observability port
EXPOSE 5000 8000

# Default command to start broker server
CMD ["npm", "run", "broker"]
