package io.astradb.node.transport;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Resolves Raft peer identifiers to their HTTP base addresses. */
public final class RaftPeerAddressProvider {

  private final Map<String, URI> addresses;

  public RaftPeerAddressProvider(String configuredAddresses) {
    Map<String, URI> parsedAddresses = new HashMap<>();

    if (configuredAddresses != null && !configuredAddresses.isBlank()) {
      for (String mapping : configuredAddresses.split(",")) {
        String[] parts = mapping.trim().split("=", 2);

        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
          throw new IllegalArgumentException("Invalid Raft peer address mapping: " + mapping);
        }

        URI address = URI.create(parts[1].trim());

        if (!("http".equalsIgnoreCase(address.getScheme())
                || "https".equalsIgnoreCase(address.getScheme()))
            || address.getHost() == null
            || address.getPort() < 1
            || address.getRawQuery() != null
            || address.getRawFragment() != null) {
          throw new IllegalArgumentException("Invalid HTTP address for peer: " + parts[0]);
        }

        if (parsedAddresses.putIfAbsent(parts[0].trim(), address) != null) {
          throw new IllegalArgumentException("Duplicate Raft peer address: " + parts[0]);
        }
      }
    }

    this.addresses = Map.copyOf(parsedAddresses);
  }

  public URI getAddress(String peerId) {
    Objects.requireNonNull(peerId, "Peer ID must not be null");

    URI address = addresses.get(peerId);

    if (address == null) {
      throw new IllegalArgumentException("No HTTP address configured for Raft peer: " + peerId);
    }

    return address;
  }
}
