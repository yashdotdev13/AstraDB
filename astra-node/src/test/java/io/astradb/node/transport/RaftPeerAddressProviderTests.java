package io.astradb.node.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.junit.jupiter.api.Test;

class RaftPeerAddressProviderTests {

  @Test
  void shouldResolveConfiguredPeerAddress() {
    RaftPeerAddressProvider provider =
        new RaftPeerAddressProvider("node-2=http://localhost:8082,node-3=http://localhost:8083");

    assertEquals(URI.create("http://localhost:8082"), provider.getAddress("node-2"));
    assertEquals(URI.create("http://localhost:8083"), provider.getAddress("node-3"));
  }

  @Test
  void shouldRejectUnknownPeer() {
    RaftPeerAddressProvider provider = new RaftPeerAddressProvider("node-2=http://localhost:8082");

    assertThrows(IllegalArgumentException.class, () -> provider.getAddress("node-3"));
  }

  @Test
  void shouldRejectDuplicatePeerIds() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RaftPeerAddressProvider(
                "node-2=http://localhost:8082,node-2=http://localhost:8083"));
  }

  @Test
  void shouldRejectMappingWithoutEqualsSeparator() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RaftPeerAddressProvider("node-2-http://localhost:8082"));
  }

  @Test
  void shouldRejectBlankPeerId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RaftPeerAddressProvider(" =http://localhost:8082"));
  }

  @Test
  void shouldRejectUnsupportedScheme() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RaftPeerAddressProvider("node-2=ftp://localhost:8082"));
  }

  @Test
  void shouldRejectAddressWithoutPort() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RaftPeerAddressProvider("node-2=http://localhost"));
  }

  @Test
  void shouldRejectAddressWithQueryParameters() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RaftPeerAddressProvider("node-2=http://localhost:8082/path?mode=test"));
  }

  @Test
  void shouldAllowEmptyConfiguration() {
    RaftPeerAddressProvider provider = new RaftPeerAddressProvider("");

    assertThrows(IllegalArgumentException.class, () -> provider.getAddress("node-2"));
  }
}
