class base_intermediator:
    def __init__(self, auth_key):
        self.auth_key=auth_key
        self.authenticate()

    def authenticate(self):
        pass

    def change_key(self, auth_key):
        self.auth_key=auth_key
        self.authenticate()

    def place_order(self):
        pass

    def poll_for_success(self):
        pass

    def get_images_links(self):
        pass

    def download_files(self):
        pass
