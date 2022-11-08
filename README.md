## Development Roadmap :nut_and_bolt:

### Overview

- Milestone 1 — Implement power-users search module
- Milestone 2 - Implement look-a-like module
- Milestone 3 - Implement the users' similarity module and the users' activity module


### Milestone 1 - Implement power-users search module
We will create a module that will search for power users of a smart contract, given the indexed blockchain data in described format.  


### Milestone 2 - Implement look-a-like module

We will create a module that will create a look-a-like (embedded vector) representation of power users in user space

### Milestone 3 - Implement the users' similarity module and the users' activity module

- Users similarity module: We will create a module that will find the most similar users in the whole NEAR blockchain given a specific set of look-a-likes
- Similar contracts search module : We will create a module that will find and count how many potential similar to look-a-like users interacted with each smart contract (with at least one interaction during a given past period)
- API setup : We will run an API, which Mintbase users or Mintbase itself can use or integrate into Mintbase UI

## Future Plans

In the future, our primary goal is to develop protocol architecture and economics concepts based on user research.
The protocol should allow building tools like (but lot limited to):
- Recommender system for NFT marketplaces
- Personalized user incentives
- Affinity analysis
- Product analytics tools
- Open-source zero-party tracker
- Competitor analysis
- Cohort analysis
- etc.

Our roadmap includes delivering:
- General power-users analysis module that can be used as a separate open-source product
- Rewrite data extraction module (not covered by this grant application) to user NEAR Lake data (for Google Cloud) instead of NEAR Indexer for Explorer data
- Blend web2 & web3 data to enhance user acquisition
- Provide API to access and manage marketing data
