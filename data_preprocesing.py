import pandas as pd
import re

rcc_train = pd.read_csv("train/rcc_train.csv")
'''Vamos a trabajar ahora con la base de RCC:
El principal problema que tiene esta base es su estructura temporal, que consiste de múltiples series de tiempo, una por cada producto en cada banco.'''

rcc_train[(rcc_train.key_value == 4) & (rcc_train.cod_instit_financiera == 33)].sort_values("codmes")
rcc_train[(rcc_train.key_value == 4) & (rcc_train.cod_instit_financiera == 61)].sort_values("codmes")

'''Primero discretizamos los días de atraso para poder manipularla mejor.'''
bins = [-1, 0, 10, 20, 30, 60, 90, 180, 360, 720, float("inf")]
rcc_train["condicion"] = pd.cut(rcc_train.condicion, bins)
rcc_train["condicion"] = rcc_train["condicion"].cat.codes

'''¿Cómo podemos procesar rcc para extraer información útil?'''
def makeCt(df, c, aggfunc=sum):
    try:
        ct = pd.crosstab(df.key_value, df[c].fillna("N/A"), values=df.saldo, aggfunc=aggfunc)
    except:
        ct = pd.crosstab(df.key_value, df[c], values=df.saldo, aggfunc=aggfunc)
    ct.columns = [f"{c}_{aggfunc.__name__}_{v}" for v in ct.columns]
    return ct

train = []
aggfuncs = [len, sum, min, max]
for c in rcc_train.drop(["codmes", "key_value", "saldo"], axis=1):
    print("haciendo", c)    
    train.extend([makeCt(rcc_train, c, aggfunc) for aggfunc in aggfuncs])

train = pd.concat(train, axis=1)
train.to_csv(r'/home/bruno/Developing_Learning/Kaggle_Competetion/Interbank_Datathon/train/rcc_train_process.csv')
del train

rcc_test= pd.read_csv("test/rcc_test.csv")

rcc_test["condicion"] = pd.cut(rcc_test.condicion, bins)
rcc_test["condicion"] = rcc_test["condicion"].cat.codes

test = []
aggfuncs = [len, sum, min, max]
for c in rcc_train.drop(["codmes", "key_value", "saldo"], axis=1):
    print("haciendo", c)
    test.extend([makeCt(rcc_test, c, aggfunc) for aggfunc in aggfuncs])

del rcc_test
test = pd.concat(test, axis=1)
test.to_csv(r'/home/bruno/Developing_Learning/Kaggle_Competetion/Interbank_Datathon/test/rcc_test_process.csv')
del test

se_train = pd.read_csv("train/se_train.csv", index_col="key_value")
sunat_train = pd.read_csv("train/sunat_train.csv")
train = pd.read_csv("train/rcc_train_process.csv")
# En este caso, no es una serie de tiempo pero tenemos multiples filas por cada persona, dadas por la multiplicidad de rubros anotados
pd.crosstab(sunat_train.key_value, sunat_train.ciiu)

'''Incorporamos la Información adicional existente en las tablas socio económicas y del censo. Es un simple join porque ambas tienen key_value únicos
Por el momento no incorporamos la información tributaria porque requiere un tratamiento más complejo que queda para futuras revisiones'''
train = train.join(pd.crosstab(sunat_train.key_value, sunat_train.ciiu)).join(se_train)

del sunat_train, se_train

se_test= pd.read_csv("test/se_test.csv", index_col="key_value")
sunat_test= pd.read_csv("test/sunat_test.csv")
test = pd.read_csv("test/rcc_test_process.csv")

test = test.join(pd.crosstab(sunat_test.key_value, sunat_test.ciiu)).join(se_test)

del sunat_test, se_test

#Por la naturaleza de las variables creadas, nos aseguramos que solo se utilicen variables existentes en ambos conjuntos de datos (train y test)
keep_cols = list(set(train.columns).intersection(set(test.columns)))
train = train[keep_cols]
test = test[keep_cols]
len(set(train.columns) - set(test.columns)) , len(set(test.columns) - set(train.columns))

train.columns = [str(c) for c in train.columns]
train = train.rename(columns = lambda x:re.sub('[^A-Za-z0-9_-]+', '', x))

test.columns = [str(c) for c in test.columns]
test = test.rename(columns = lambda x:re.sub('[^A-Za-z0-9_-]+', '', x))

train.to_csv(r'/home/bruno/Developing_Learning/Kaggle_Competetion/Interbank_Datathon/train/train.csv')
test.to_csv(r'/home/bruno/Developing_Learning/Kaggle_Competetion/Interbank_Datathon/test/test.csv')
del test, train

