## What is MetaSo?

MetaSo is a decentralized social network platform based on the MetaID protocol, where all social data (tweets, comments, likes, donations, etc.) are permanently recorded on the blockchain as Pins. MetaSo Man Indexer is responsible for discovering, parsing, and indexing this on-chain data, making it quickly retrievable and usable.

## Core Features

### 1. Social Data Indexing
- **Buzz Feed**: Real-time indexing of user-published tweets with support for newest, recommended, hot, and other sorting methods
- **Interaction Data**: Complete recording of social interactions like likes, comments, and donations
- **User Relations**: Track follow relationships with support for follow list queries
- **Full-Text Search**: Integrated Jieba Chinese word segmentation for content search

### 2. Multi-Chain Support
- Bitcoin (BTC) mainnet, testnet, and regtest
- MicroVisionChain (MVC)
- Extensible to other UTXO model blockchains

### 3. Real-Time Data Synchronization
- Index on-chain data in block height order
- Real-time mempool data updates
- ZMQ message push mechanism for zero-latency data delivery

### 4. Comprehensive API Services
- **Social Data API**: `/social/buzz/*` - Provides tweet feeds, search, interactions
- **User Data API**: `/host/*` - User information and statistics queries
- **Asset Protocol API**: `/ft/*` - MRC20 token information queries
- **Management API**: `/metaso/settings/*` - Blocklist and recommendation management

### 5. Data Statistics & Analytics
- User activity statistics (DAU/MAU)
- Content popularity ranking algorithm
- Recommendation system support
- PEV (Pin Exposure Value) metric calculation

## Supported MetaSo Protocols

This indexer is specifically optimized for indexing the following MetaSo protocols:

- **Buzz Protocol**: MetaSo's core tweet protocol
- **PayLike Protocol**: On-chain likes and donations
- **PayComment Protocol**: Comments and replies
- **Follow Protocol**: User follow relationships
- **MRC20 Protocol**: Fungible tokens (e.g., SPACE)
- **MRC721 Protocol**: NFT assets

## Quick Start

### Dependencies

1. **libzmq** - For real-time mempool data push
   ```bash
   # Ubuntu/Debian
   apt-get install libzmq3-dev
   
   # macOS
   brew install zmq
   
   # Fedora
   dnf install zeromq-devel
   ```

2. **Golang >= 1.20**

3. **MongoDB** - Recommended for MetaSo data storage
   ```bash
   # Quick start with Docker
   docker run -d -p 27017:27017 --name mongodb \
     -e MONGO_INITDB_ROOT_USERNAME=root \
     -e MONGO_INITDB_ROOT_PASSWORD=123456 \
     mongo:latest
   ```

### Build

```bash
# Clone the repository
git clone https://github.com/metaid-developers/metaso-man-indexer.git
cd metaso-man-indexer

# Install dependencies
go mod tidy

# Build
go build -o manindexer
```

### Configuration

Create a `config.toml` configuration file:

```toml
# Sync configuration
[sync]
syncAllData = true              # Whether to sync all Pin data
syncBeginTime = ""              # Sync start time (empty for current)
syncEndTime = ""                # Sync end time (empty for continuous)
syncProtocols = ["payLike", "payComment"]  # Specific protocols to sync

# Protocol definitions (example: PayLike protocol)
[protocols]
  [protocols.payLike]
  fields = [
    {name = "isLike", class = "string", length = 1},
    {name = "likeTo", class = "string", length = 100}
  ]
  indexes = [
    {fields = ["likeTo"], unique = false},
    {fields = ["pinId"], unique = true},
    {fields = ["pinAddress"], unique = false}
  ]

# BTC chain configuration
[btc]
initialHeight = 840000          # Starting block height
rpcHost = "127.0.0.1:8332"     # Bitcoin RPC address
rpcUser = "your_rpc_user"       # RPC username
rpcPass = "your_rpc_password"   # RPC password
rpcHttpPostMode = true
rpcDisableTLS = true
zmqHost = "tcp://127.0.0.1:28332"  # ZMQ push address

# Database configuration
[mongodb]
mongoURI = "mongodb://root:123456@127.0.0.1:27017"
dbName = "metaso_mainnet"
poolSize = 200
timeOut = 20

# Web service configuration
[web]
port = ":7777"                  # API service port
pemFile = ""                    # SSL certificate (optional)
keyFile = ""                    # SSL key (optional)
```

### Run the Service

```bash
# Connect to BTC mainnet with MongoDB and start web service
./manindexer -chain=btc -database=mongo -server=1

# Connect to BTC testnet
./manindexer -chain=btc -database=mongo -server=1 -test=1

# Connect to MVC chain
./manindexer -chain=mvc -database=mongo -server=1
```

### Docker Deployment (Recommended)

```bash
# 1. Download configuration files
wget https://github.com/metaid-developers/man-indexer/blob/main/docker/docker-compose.yml
wget https://github.com/metaid-developers/man-indexer/blob/main/docker/.env

# 2. Edit .env file with your Bitcoin RPC node information

# 3. Start the service
docker-compose up -d

# 4. View logs
docker-compose logs -f manindexer
```

After the service starts, access the MetaSo API at `http://localhost:7777`.

## Command Line Parameters

```bash
-chain string
    Blockchain to index (options: btc, mvc) (default: "btc")

-database string
    Database to use (options: mongo, pebble, postgresql) (default: "mongo")

-server string
    Start Web API service (1: enable, 0: disable) (default: "1")

-test string
    Network type (0: mainnet, 1: testnet, 2: regtest) (default: "0")
```

Examples:
```bash
# BTC mainnet + MongoDB + API service
./manindexer -chain=btc -database=mongo -server=1 -test=0

# BTC testnet + PebbleDB
./manindexer -chain=btc -database=pebble -server=1 -test=1

# MVC mainnet
./manindexer -chain=mvc -database=mongo -server=1
```

## API Usage Examples

### Get Latest Tweet Feed

```bash
curl "http://localhost:7777/social/buzz/newest?size=20"
```

Response example:
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "list": [
      {
        "id": "abc123...",
        "content": "Hello MetaSo!",
        "metaid": "d1f2e3...",
        "timestamp": 1705234567,
        "likeCount": 42,
        "commentCount": 8
      }
    ],
    "total": 1000,
    "lastId": "abc123..."
  }
}
```

### Search Content

```bash
curl "http://localhost:7777/social/buzz/search?keyword=bitcoin&size=20"
```

### Get Recommended Content

```bash
curl "http://localhost:7777/social/buzz/recommended?size=20"
```

### Query User Information

```bash
curl "http://localhost:7777/host/info?metaid=d1f2e3..."
```

### Get MRC20 Token List

```bash
curl "http://localhost:7777/ft/mrc20/address/deploy-list?address=1A1zP1..."
```

For complete API documentation, visit: `http://localhost:7777/swagger/index.html`

## MetaSo Data Structures

### Buzz Tweet

```json
{
  "id": "Tweet Pin ID",
  "metaid": "User MetaID",
  "address": "User address",
  "content": "Tweet content",
  "contentType": "text/plain",
  "timestamp": 1705234567,
  "genesisHeight": 840000,
  "genesisTransaction": "Transaction hash",
  "likeCount": 42,
  "commentCount": 8,
  "donateAmount": 1000
}
```

### Like/Donation

```json
{
  "isLike": "1",
  "likeTo": "Target Pin ID",
  "metaid": "Actor MetaID",
  "timestamp": 1705234567
}
```

### Comment

```json
{
  "content": "Comment content",
  "commentTo": "Target Pin ID",
  "metaid": "Commenter MetaID",
  "timestamp": 1705234567
}
```

## Building Applications

Workflow for developing applications based on MetaSo Man Indexer:

1. **Deploy Indexer**: Deploy and start the indexer service following the steps above
2. **Wait for Sync**: First run requires syncing data from specified block height
3. **Integrate API**: Use the provided RESTful API to query social data
4. **Use SDK**: Combine with MetaID SDK for on-chain data functionality
5. **Test Deployment**: Complete development and testing on testnet first
6. **Mainnet Release**: Switch to mainnet configuration and go live

Recommended Tech Stack:
- Frontend: React/Vue + MetaID SDK
- Backend: MetaSo Man Indexer API
- Wallet: MetaLet browser extension

## MetaSo Browser

MetaSo Man Indexer includes a complete data browser for intuitively viewing on-chain social data.

### Live Demo

Visit https://man.metaid.io to experience the full functionality.

### Features

#### 1. Global Search
Supports searching by:
- MetaID: Query all user data
- Pin ID: Query specific tweets or data
- Address: Query all activities related to an address
- Content keywords: Full-text search of tweet content (supports Chinese word segmentation)

#### 2. Buzz Feed
- Display all tweets in reverse chronological order
- Click tweets to view detailed information, comments, and interaction data
- Example: https://man.metaid.io/pin/9bc429654d35a11e5dde0136e3466faa03507d7377769743fafa069e38580243i0

#### 3. MetaID User List
- Display all MetaID users
- Sort by creation time
- Click to view user's creation transaction and all content

#### 4. Block Explorer
- Display all blocks containing MetaSo data
- Sort by block height in descending order
- Click to view all social activities in that block
- Example: https://man.metaid.io/block/844453

#### 5. Mempool Monitor
- Real-time display of unconfirmed MetaSo data
- Automatically removed from list after data is on-chain
- Monitor latest on-chain activities

### Local Browser Deployment

```bash
# Method 1: Run compiled program directly
./manindexer -chain=btc -database=mongo -server=1

# Method 2: Use Docker
docker-compose up -d

# Method 3: Compile and run from source
go build -o manindexer
./manindexer -server=1
```

The browser runs on port 7777 by default, accessible at:
- Local: http://localhost:7777
- Remote: http://your-server-ip:7777

To customize the port, modify in config.toml:
```toml
[web]
port = ":8080"  # Change to your desired port
```

## Built-in CLI Wallet

MetaSo Man Indexer provides a fully-featured command-line wallet tool for interacting with the MetaID protocol and MRC20 tokens.

### Usage

```bash
# Enter project directory
cd metaso-man-indexer

# View help
./man-cli help
```

### Available Commands

```bash
# Initialize wallet
./man-cli init-wallet

# Query balance
./man-cli getbalance

# Query UTXO list
./man-cli utxo

# Query MRC20 token balance
./man-cli mrc20balance

# MRC20 operations (deploy, mint, transfer)
./man-cli mrc20op deploy --tick=SPACE --supply=21000000
./man-cli mrc20op mint --tick=SPACE --amount=1000
./man-cli mrc20op transfer --tick=SPACE --amount=100 --to=1A1zP1...

# View version
./man-cli version
```

**Note**: The CLI wallet requires a local Bitcoin RPC node configuration with rpcUser and rpcPass correctly set in config.toml.

## JSON API Documentation

### MetaSo Social API

Complete MetaSo API documentation:

- **Online Swagger Docs**: http://localhost:7777/swagger/index.html
- **Basic APIs**:
  1. [PIN API](https://github.com/metaid-developers/man-indexer/wiki/JSON-API-%E2%80%90-PIN) - Tweet data queries
  2. [MetaID API](https://github.com/metaid-developers/man-indexer/wiki/JSON-API-%E2%80%90-MetaID) - User information queries
  3. [Follow API](https://github.com/metaid-developers/man-indexer/wiki/JSON-API-%E2%80%90-Follow) - Follow relationship queries
  4. [MRC20 API](https://github.com/metaid-developers/man-indexer/wiki/JSON-API-%E2%80%90-MRC20) - Token data queries

### General Query Interface

Provides a flexible general query interface for querying any protocol data.

**Endpoint**: `POST /api/generalQuery`

**Request Parameters**:
```json
{
    "collection": "Collection name to query, e.g.: pins, paylike",
    "action": "Operation type: get (query) | count (count) | sum (sum)",
    "filterRelation": "Query condition relation: and | or",
    "field": ["Fields to return in query, required for sum operation"],
    "filter": [
        {
            "operator": "Condition operator: = | > | >= | < | <=",
            "key": "Field name",
            "value": "Field value"
        }
    ],
    "cursor": 0,
    "limit": 20,
    "sort": ["Field name", "Sort order: asc | desc"]
}
```

**Example: Query all likes for a tweet**
```json
{
    "collection": "paylike",
    "action": "get",
    "filterRelation": "and",
    "filter": [{
        "operator": "=",
        "key": "likeTo",
        "value": "9fec9e5eb879049bd8403ffa45ca0e2756b6c14434b507ccdaf7771d5ec4edf9i0"
    }],
    "cursor": 0,
    "limit": 99999,
    "sort": []
}
```

**Success Response**:
```json
{
    "code": 0,
    "message": "success",
    "data": [...]
}
```

**Error Response**:
```json
{
    "code": -1,
    "message": "Data not found",
    "data": null
}
```

## Performance Optimization

### Database Selection

- **MongoDB**: Recommended for production, supports complex queries and aggregation operations
- **PebbleDB**: Suitable for lightweight deployments, excellent performance but limited query capabilities
- **PostgreSQL**: Suitable for scenarios requiring relational databases

### Index Configuration

Recommended MongoDB indexes:
```javascript
// Buzz tweet collection
db.pins.createIndex({"timestamp": -1})
db.pins.createIndex({"metaid": 1, "timestamp": -1})
db.pins.createIndex({"path": 1})

// Like collection
db.paylike.createIndex({"likeTo": 1})
db.paylike.createIndex({"metaid": 1})

// Comment collection
db.paycomment.createIndex({"commentTo": 1})
```

### Sync Strategy

1. **Initial Sync**: Start from recent block height (e.g., last 3 months)
2. **Incremental Sync**: Enable ZMQ real-time push
3. **Periodic Full Sync**: Periodically re-index to ensure data integrity

## Troubleshooting

### Common Issues

**1. ZMQ Connection Failed**
```bash
# Check if Bitcoin node has ZMQ enabled
bitcoin-cli getzmqnotifications

# Ensure zmqHost in config.toml is correct
zmqHost = "tcp://127.0.0.1:28332"
```

**2. MongoDB Connection Timeout**
```bash
# Check if MongoDB is running
docker ps | grep mongo

# Test connection
mongosh "mongodb://root:123456@127.0.0.1:27017"
```

**3. Slow Sync Speed**
- Increase RPC concurrency
- Use local Bitcoin node
- Optimize database indexes

**4. High Memory Usage**
- Limit poolSize
- Use PebbleDB instead of MongoDB
- Enable sharded storage

## Docker Build

```bash
# Build image
sudo docker build -t man-indexer:0.1 .

# Run container
sudo docker run -d --name man-indexer --network=host --restart=always -m 2g man-indexer:0.1
```

## Contributing & Support

### License

This project is licensed under the MIT License.

### How to Contribute

Issues and Pull Requests are welcome!

- GitHub: https://github.com/metaid-developers/man-indexer
- Documentation: https://github.com/metaid-developers/man-indexer/wiki
- Community: Join MetaSo Discord

### Contact

- Website: https://metaso.io
- Twitter: @MetaSoProtocol
- Telegram: MetaSo Community

## Related Resources

- **MetaID Protocol Docs**: https://metaid.io
- **MetaLet Wallet**: https://metalet.io
- **MetaSo Application**: https://metaso.io
- **MRC20 Standard**: https://github.com/metaid-developers/MRC20

---

**Building the Future of Decentralized Social | Powered by MetaSo**
