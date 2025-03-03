// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title Cross-Chain Asset Tracker with ZKP Commitments
 * @dev Manages asset locks across EVM chains with proof verification
 */
contract CrossChainTracker {
    struct AssetLock {
        address owner;
        uint256 lockedValue;
        bytes32 zkProofHash;
        uint256 expirationBlock;
    }

    mapping(uint256 => mapping(bytes32 => AssetLock)) public chainAssets;
    
    event AssetLocked(
        uint256 indexed originChainId,
        bytes32 indexed assetHash,
        address owner,
        uint256 value
    );

    modifier onlyValidChain(uint256 chainId) {
        require(chainId != block.chainid, "Cannot lock on native chain");
        _;
    }

    function lockAsset(
        bytes32 assetHash,
        uint256 value,
        bytes32 zkProofHash,
        uint256 targetChainId
    ) external payable onlyValidChain(targetChainId) {
        chainAssets[targetChainId][assetHash] = AssetLock(
            msg.sender,
            value,
            zkProofHash,
            block.number + 100000
        );
        emit AssetLocked(targetChainId, assetHash, msg.sender, value);
    }

    function verifyProof(
        uint256 chainId,
        bytes32 assetHash,
        bytes calldata zkProof
    ) external view returns (bool) {
        bytes32 storedHash = chainAssets[chainId][assetHash].zkProofHash;
        return keccak256(zkProof) == storedHash;
    }
}
