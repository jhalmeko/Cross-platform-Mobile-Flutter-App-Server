import 'dart:async';
import 'package:grpc/grpc.dart';
import 'generated/moviematch.pbgrpc.dart';

// MovieMatchService-luokka toteuttaa MovieMatchServiceBase:n, joka on gRPC-palvelun perusta.
class MovieMatchService extends MovieMatchServiceBase {
  // clients: Tallentaa jokaisen käyttäjän StreamControllerin, jotta viestejä voidaan lähettää takaisin Käyttäjille.
  final Map<String, StreamController<StateMessage>> clients = {};
  
  // userValues: Tallentaa jokaisen käyttäjän lähettämät tiedot (data) listana.
  final Map<String, List<String>> userValues = {};

  // streamState: Tämä on gRPC-metodi, joka käsittelee Käyttäjien lähettämiä viestejä ja palauttaa reaaliaikaisia vastauksia.
  @override
  Stream<StateMessage> streamState(
    ServiceCall call, // gRPC-palvelukutsu
    Stream<StateMessage> request, // Käyttäjän lähettämä viestivirta
  ) {
    // Luodaan uusi StreamController, joka hallitsee palvelimen lähettämiä viestejä Käyttäjille.
    final controller = StreamController<StateMessage>();
    String? currentUser; // Tallentaa nykyisen käyttäjän tunnisteen.

    // Kuunnellaan Käyttäjän lähettämiä viestejä.
    request.listen(
      (msg) {
        // msg sisältää käyttäjän tunnisteen ja datan.
        currentUser = msg.user; // Tallennetaan nykyinen käyttäjä.
        clients[msg.user] = controller; // Liitetään käyttäjä StreamControlleriin.

        // Tallennetaan käyttäjän lähettämä data userValues-muuttujaan.
        if (userValues.containsKey(msg.user)) {
          userValues[msg.user]?.add(msg.data); // Lisätään data olemassa olevaan listaan.
        } else {
          userValues[msg.user] = [msg.data]; // Luodaan uusi lista, jos käyttäjää ei ole vielä tallennettu.
        }

        // Tulostetaan palvelimen nykyinen muistissa oleva tietokanta.
        print("Current server in-memory db state: $userValues");

        // Tarkistetaan, löytyykö muita käyttäjiä, joilla on sama data.
        for (var entry in userValues.entries) {
          if (entry.key != msg.user && entry.value.contains(msg.data)) {
            // Jos löytyy, luodaan viesti, joka ilmoittaa yhteensopivuudesta.
            final matchMessage =
                StateMessage()
                  ..user = 'server' // Viestin lähettäjä on palvelin.
                  ..data = msg.data; // Yhteensopiva data.

            // Lähetetään viesti molemmille käyttäjille.
            clients[entry.key]?.add(matchMessage);
            clients[msg.user]?.add(matchMessage);
          }
        }
      },
      onDone: () {
        // Kun Käyttäjä lopettaa yhteyden, poistetaan käyttäjä muistista.
        if (currentUser != null) {
          clients.remove(currentUser); // Poistetaan käyttäjän StreamController.
          userValues.remove(currentUser); // Poistetaan käyttäjän data.
        }
      },
    );

    // Palautetaan StreamController, jotta Käyttäjä voi vastaanottaa viestejä.
    return controller.stream;
  }
}