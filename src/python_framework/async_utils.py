from threading import Thread


def fire_and_forget(f):
    def wrapped(*args, **kwargs):
        Thread(target=f, args=args, kwargs=kwargs).start()

    return wrapped

