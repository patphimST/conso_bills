from functions import *
import traceback

# Fonction principale pour exécuter les différentes étapes
def main():
    try:
        # Étape 1: Exécuter la fonction get_conso
        print("Exécution de get_conso()")
        get_conso()

        # Étape 2: Exécuter la fonction warning
        print("Exécution de warning()")
        warning()

        # Étape 3: Exécuter la fonction get_base
        print("Exécution de get_base()")
        get_base()

        # Étape 4: Exécuter la fonction get_tarif
        print("Exécution de get_tarif()")
        get_tarif()

        # Étape 5: Exécuter la fonction get_entities
        print("Exécution de get_entities()")
        get_entities()

        # Étape 6: Exécuter la fonction get_entities_unactive
        print("Exécution de get_entities_unactive()")
        get_entities_unactive()

        # Étape 7: Exécuter la fonction get_portefeuille
        print("Exécution de get_portefeuille()")
        get_portefeuille()

        # Étape 8: Exécuter la fonction merge_all
        print("Exécution de merge_all()")
        merge_all()

        # Étape 9: Exécuter la fonction update_drive
        print("Exécution de update_drive()")
        update_drive()

    except Exception as e:
        # En cas d'erreur dans l'une des étapes, on capture l'exception
        raise RuntimeError(f"Erreur lors de l'exécution du script principal: {e}")

# Gestion globale des erreurs et envoi d'e-mail de statut
error_message = None

try:
    # Exécuter la fonction principale
    main()
    status = "✅ Success"
    error = "OK"
except Exception as e:
    # Capturer le traceback complet en cas d'échec
    error_message = traceback.format_exc()
    status = "❌ Failed"
    error = error_message

# Envoyer un e-mail avec le statut final
envoi_email(status, error)
