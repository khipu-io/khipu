Fork happened
https://www.reddit.com/r/ethereum/comments/5es5g4/a_state_clearing_faq/
 
https://ethereum.stackexchange.com/questions/9883/why-is-my-node-synchronization-stuck-extremely-slow-at-block-2-306-843
Mar 5 2017


The state cleaning was announced by Vitalik Buterin in the tweet State clearing 100% complete dated 23:07 Nov 29 2016. This time corresponds to block 2,718,436.

The Clearing Contract can be found at 0xe9c9068240d8450da314f60804debfc194b72309. There was over 10,000 transactions involved in clearing the state. The first transaction to Sweeper.sol was at block 2,675,055 in transaction 0x884d0fc7.... The last transaction to Sweeper.sol was at block 2,700,301 in transaction 0x6b651cfd....

The Sweeper.sol Contract can be found at 0xa43ebd8939d8328f5858119a3fb65f65c864c6dd. There was 35370 transactions involved in clearing the state. The first state clearing transaction was at block 2,675,055 in transaction 0x884d0fc7.... The last state clearing transaction was at block 2,717,576 in transaction 0xbf78cc00....

Note that from #2,675,000 when the 4th hard fork occurred, the syncing slows down as the node clients are clearing out the empty accounts created by the account bloat transactions. 
