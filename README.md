# LinkArchiverAPIMock
Back-end API/Services for a basic URL web page archiver I've been working on, to practice more with Docker, Kafka &amp; databases

Currently the service allows clients to paste a text snippet or the content from a given URL temporarily, and recieve a unique id. Using the id the client will either get the associated content (before an expiration time). In the case of pasting content from a URL though, the client may instead recieve a status message if the service is still (or has failed) processing the upload request. 