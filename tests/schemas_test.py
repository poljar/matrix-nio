from nio.schemas import check_user_id


class TestClass:
    def test_check_user_id__valid(self):
        assert check_user_id("@foobar:example.com") is True

    def test_check_user_id__invalid(self):
        try:
            check_user_id("foobar:example.com")
        except ValueError:
            pass
        try:
            check_user_id("@foobar@example.com")
        except ValueError:
            pass
        try:
            check_user_id("@FOOBAR:example.com")
        except ValueError:
            pass
        try:
            check_user_id("https://example.com")
        except ValueError:
            pass
        try:
            check_user_id("@foobar:")
        except ValueError:
            pass
