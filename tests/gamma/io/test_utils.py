from gamma.io._utils import progress


def test_progress():
    update, close = progress(total=2, force_tty=True)

    update()
    update()
    close()

    # no asserts, just ensure it does not break
