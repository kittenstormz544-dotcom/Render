# Use a lightweight Node.js base image
FROM node:20-alpine

# Install FFmpeg. This is the crucial step that adds the video processing capability.
RUN apk add --no-cache ffmpeg

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json (if exists) to install dependencies
COPY package*.json ./

# Install the server dependencies (only Express is needed)
RUN npm install

# Copy the server logic
COPY server.js .

# The server will run on port 3000 by default (standard for these platforms)
EXPOSE 3000

# Start the server
CMD [ "node", "server.js" ]
