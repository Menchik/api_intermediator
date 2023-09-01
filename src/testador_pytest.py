import os
import pytest

from planet_api import planet_mm

class PlanetAPITest:

    def test_set_mosaic(self):
        # Cria um objeto planet_mm
        PL_API_KEY = os.environ.get('PL_API_KEY')
        planet_object = planet_mm(PL_API_KEY)

        # Configura o mosaico como "mosaico1"
        planet_object.set_mosaic(0)

        # Verifica se o mosaico está configurado corretamente
        assert planet_object.order_params['products'][0]['mosaic_name'] == "mosaico1"

# pytest -q src/testador_pytest.py