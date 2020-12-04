# Introduction

We need to understand the implications of using different sFlow sampling rates on the accuracy
of traffic measurement.

Until now, we have been incapable of measuring or even estimating per-IP address transit usage. We currently
use sFlow on our edge routers to sample 1 in 25,000 packets flowing through the transit circuits.

There will, inevitably, be a certain amount of error in the figures inferred from these samples. We want to
know approximately what this margin of error is for the current 1:25,000 sampling ratios and other suggested 
ones (1:10,000, 1:5,000, 1:2,500, 1:1,000, 1:250)

We need to understand this in the context of the "shape" of traffic that we route, both in terms of packet 
sizes but also the fact that different IP addresses will likely generate wildly different levels of
traffic, with what we expect to be a Pareto distribution with a very small number of large users and a long tail
of smaller ones. 

# How do we go about this?

## Getting the _shape_ of our typical traffic

We need to understand the typical shape of traffic on the network in terms of the frequencies of packet sizes, 
and then generate random data of that shape. We also need to understand how this is distributed amongst our 
various IP addresses.  

To this end, we have taken an sFlow index for a typical day
(18th November 2020) and from it extracted those FLOW records where there is IP information (i.e. discarding
CNTR records and those FLOW records for MPLS traffic where there is no IP information).

The information contains (amongst other things):
* the IP address (source ip for an outbound packet, destination ip for an inbound packet)
* the size of the packet in bytes

This data is stored in JSON format.

We analyse this data to get counters for the various ip addresses and a histogram for the various
packet sizes.

## Getting the _shape_ of our users

We would reasonably expect the different IP addresses on our network to follow a Pareto distribution for the amount 
of traffic sent and received. (That is, a small number of very large users and a long tail consisting of a 
large number of small users.)

Again, we analyse this distribution in terms of the number of packets sent by each unique IP address.[^1]

[^1]: We are, of course, only interested in IP addresses for our own equipment, so we consider the source IP 
address for outbound and the destination IP address for inbound. 
 
# The simulation

The simulation generates large blocks (typically 1M) random numbers with the same distribution as our packet 
lengths and a corresponding number of IP addresses picked from a random sample (typically 1M). Several of these blocks
are generated, and a simulation of 1Tn packets has been performed. 

These random "packets" are then sampled at different rates: 1:50,000; 1:25,000; 1:10,000; 1:5,000; 1:2,500; 1:1,000; 
1:250 and the total number of bytes computed for each of the different sample rates. These data are written to a 
Postgresql database for subsequent analyis. 

# Analysis

The data is analysed to determine the margin of error (MoE) for the different sampling rates for the different deciles 
of customer size.   