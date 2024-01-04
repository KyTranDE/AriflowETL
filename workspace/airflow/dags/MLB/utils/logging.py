def logger(code, message, endl = '\n'):
    if code == "error":
        color_scheme = "\033[91m[ERROR]\033[0m"
    elif code == "success":
        color_scheme = "\033[92m[SUCCESS]\033[0m"
    elif code == "warning":
        color_scheme = "\033[93m[WARNING]\033[0m"
    else:
        color_scheme = "\033[94m[INFO]\033[0m"
    
    print(color_scheme + " " + message, end = endl)

B = '\033[94m'
Y = '\033[93m'
G = '\033[92m'
R = '\033[91m'
BOLD = '\033[1m'
E = '\033[0m'

def FAIL(msg='ERROR'):
    return R + str(msg) + E

def OK(msg='SUCCESS'):
    return G + str(msg) + E

def WARN(msg='WARNING'):
    return Y + str(msg) + E

def TEXT(msg=''):
    return B + f"{msg}" + E

def TEXT_BOLD(msg=''):
    return BOLD + f"{msg}" + E

def status(idx, n):
    pc = round((idx+1)/n*100, 2)
    if int(pc)*1.0 == pc:
        pc = int(pc)
    return f'({pc}%, {idx+1}/{n})'