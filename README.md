# Predykcja zapalenia wątroby typu C u pacjentów za pomocą Apache Spark
# Wstęp 
Wątroba to organ, który ma szczególną budowę anatomiczną i pełni wiele różnych funkcji, bez których organizm człowieka nie mógłby sobie poradzić. W wątrobie:
*	zachodzą przemiany najważniejszych składników odżywczych, czyli białek, tłuszczów, węglowodanów, witamin i składników mineralnych
*	wytwarzana jest żółć, która jest niezbędna w procesie trawienia i wchłaniania tłuszczów w przewodzie pokarmowym
*	wytwarzany jest cholesterol
*	zachodzą procesy kluczowe w metabolicznej aktywacji witaminy D
*	neutralizowane są różne toksyny
*	magazynowane są podstawowe składniki odżywcze, witaminy A, D, B12 i ferrytyna (białko, które magazynuje żelazo w organizmie)
*	pełnione są funkcje immunologiczne.

Do głównych czynników powodujących uszkodzenie wątroby, które mogą prowadzić do dysfunkcji tego narządu, zaliczamy:
*	przewlekłe zakażenie wirusem HBV lub HCV (wirusowe zapalenie wątroby typu B i C)
*	nadmierne spożywanie alkoholu (alkoholowa marskość wątroby)
*	otyłość i zespół metaboliczny prowadzące do stłuszczenia wątroby (niealkoholowa stłuszczeniowa choroba wątroby).
*	uszkodzenie miąższu wątroby skutkujące tworzeniem się guzków regeneracyjnych i przemianą struktury tego narządu (zwłóknienie wątroby)

Przeszczep wątroby jest często metodą z wyboru leczenia niektórych chorób, a czasami jedynym sposobem ich wyleczenia. Do przeszczepu wątroby kwalifikuje się chorych z poważnymi chorobami wątroby, którzy mają mniej niż 90% szans na przeżycie roku. Większość chorób wątroby może prowadzić do marskości i to właśnie ona jest głównym wskazaniem do zabiegu. Podczas przetaczania krwi oraz transplantacji (po sprawdzeniu stanu organu), najgroźniejszymi czynnikami przenoszonymi przez krew są: wirus upośledzenia odporności (HIV), wirus zapalenie wątroby typu C (HCV) lub wirus zapalenia wątroby typu B (HBV) – zakażenia początkowo mogą być bezobjawowe, a w dalszej perspektywie mogą prowadzić do przewlekłego stanu zapalnego, a następnie zwłóknienia i marskości wątroby, które są poważnymi chorobami. Aby uniknąć transfuzji od niepożądanego dawcy, stacje krwiodawstwa oraz szpitale przed pobraniem krwi do transfuzji  sprawdzają parametry krwi dawcy. Badanie morfologii krwi i moczu dają dużo informacji, ale aby ocenić funkcję i uszkodzenie wątroby dodatkowo wykonuje się inne badania laboratoryjne, m.in. testy świadczące o uszkodzeniu komórek wątroby(hepatocytów) i testy określające funkcję wątroby. 
# Cel
Firma <b>Analit</b> specjalizująca się w analizach laboratoryjnych dla szpitali zleciła mi opracowanie modelu szybkiego wykrywania zapalenia wątroby typu C u pacjentów. Podstawowym założeniem firmy <b>Analit</b> jest, że uda się opracować model uczenia maszynowego, który skutecznie przewidzi brak przeciwwskazań do pobrania krwi oraz transplantacji wątroby, jedynie na podstawie badania krwi potencjalnego dawcy. Umożliwi to zaoszczędzenie czasu oraz środków pieniężnych (kolejne analizy będą wtedy zbędne) w przypadkach pilnego wykonania przeszczepu wątroby lub wykonania szybkiej transfuzji krwi. Ponadto, dzięki modelowi pacjenci nieświadomi dysfunkcji swojej wątroby będą mogli być szybciej zdiagnozowani i mogą poddać się szybciej leczeniu lub dalszym badaniom ukierunkowanym na choroby wątroby.
# Wyniki badań
## Apache Spark
Z uwagi na dużą ilość pacjentów będących w bazie firmy <b>Analit</b> zdecydowałem się na implementację modelu korzystając z platformy Apache Spark.</br>
Apache Spark to platforma służąca do obliczeń rozproszonych. Została ona zaprojektowana jako silnik wykonawczy, który działa zarówno w pamięci, jak i na dysku. Spark przechowuje wyniki pośrednie w pamięci, zamiast zapisywać je na dysku, co jest bardzo przydatne, szczególnie gdy trzeba wielokrotnie pracować nad tym samym zestawem danych. Dzięki przechowywaniu danych w pamięci Spark ma przewagę wydajności. Może być nawet 100 razy szybszy niż Apache Hadoop do przetwarzania danych na dużą skalę poprzez wykorzystanie w obliczeniach pamięci i innych optymalizacjach. Spark jest również szybki, gdy dane są przechowywane na dysku. Szeroka gama bibliotek Spark i możliwość obliczania danych z wielu różnych typów magazynów danych oznacza, że Spark może być stosowany do wielu różnych problemów w wielu branżach. Do uczenia maszynowego przydatna jest biblioteka MLlib, która zapewnia zestaw algorytmów maszynowych dla powszechnych technik nauki danych: klasyfikacja, regresja, filtrowanie grupowe, klastrowanie i redukcja wymiarów.
## Wstępna obróbka danych
Na początku do pracy z ramką danych inicjalizuję interfejs Spark w języku Python (projekt będę prowadzić w interfejsie PySpark).
Wczytuję dane udostępnione mi przez firmę <b>Analit</b> w formie pliku .csv.
<p align="center">
  <img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/df.JPG" />
</p>
Dane zawierają pacjentów o unikalnym numerze pacjenta (kolumna „_c0”), kolumnę diagnoza (Category), w której są 4 wartości:

* '0=Blood Donor' – zdrowy dawca krwi
* '0s=suspect Blood Donor' – dawca krwi, u którego istnieje podejrzenie występowania zapalenia wątroby typu C
* '1=Hepatitis' – pacjent chory na jedynie zapalenie wątroby typu C
* '2=Fibrosis' – pacjent chory na zwłóknienie wątroby oraz zapalenie wątroby typu C
* '3=Cirrhosis' – pacjent chory na marskość wątroby oraz zapalenie wątroby typu C

Kolumna Age określa wiek pacjenta (w latach), a kolumna Sex określa płeć (‘f’ - kobieta, ‘m’ -mężczyczna).
Kolejne kolumny zawierają stężenia produktów syntetyzowanych w wątrobie lub innych składników krwi, które są ważne dla oceny stanu wątroby. Wszystkie stężenia są w g/l, oprócz cholinesterazy (w g/ml), cholersterolu (w  mmol/l), kreatyniny oraz bilirubiny (w µmol/l):

a) ALB – albumina (białko syntetyzowane w wątrobie)

b) ALP - fosfataza zasadowa (enzym związany z nabłonkiem wątroby)

c) ALT -  aminotransferaza alaninowa (enzym syntetyzowany w wątrobie)

d) AST - aminotransferaza asparaginianowa (enzym syntetyzowany w wątrobie)

e) BIL - bilirubina (produkt rozpadu hemoglobiny, którego dezaktywacją zajmuje się wątroba)

f) CHE – cholinoesteraza (enzym syntetyzowany w wątrobie)

g) CHOL – cholersterol (prekursor m.in. hormonów steroidowych, kwasów żółciowych i witaminy D)

h) CREA – kreatynina (metabolit białka, syntetyzowany w wątrobie)

i) GGT - gamma-glutamylotransferaza (enzym związany z nabłonkiem wątroby)

j) PROT – białko całkowite we krwi


W dalszej analizie nie jest potrzebny mi ID pacjenta, zatem usuwam tę kolumnę.
Ramka danych wygląda następująco:

<p align="center">
  <img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/df_1.JPG" />
</p>

Sprawdzam jak zbalansowany jest zbiór danych w zależności od kategorii:

<p align="center">
  <img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/balanced.JPG" />
</p>

Jak widać najwięcej jest zdrowych dawców, co może oznaczać, że są świadomi swojego zdrowia.</br>
Przygotowując dane do wszystkich wektoryzacji wartości ze wszystkich kolumn dla każdego pacjenta sprawdzam spójność danych, tj. czy zawierają one wartości null lub NA.

<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/null.JPG" />
</p>

Nie mam wartosci null jako takich, ale jeszcze zamienie wartosci NA na 0.

<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/null2.JPG" />
</p>

Z uwagi na operację wektoryzacji sprawdzam też typy danych.

<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/typy.JPG" />
</p>

Wszystkie dane są niestety stringami, łącznie z poszczególnymi stężeniami zbadanych parametrów krwi. Muszę zamienić kategorie oraz płeć na int, a resztę na float.




Aby tego dokonać muszę zamienić ramkę danych zgodną ze Sparkiem na ramkę z pakietu Pandas , zamienić na następnie ponownie utworzyć ramkę zgodną z PySparkiem.</br>
Znajduję unikalne wartości kolumn Category i Sex, a następnie zamieniam na cyfry w następujący sposób:
a) 'Category'

* '0=Blood Donor' (zdrowy dawca krwi) na 0
* '0s=suspect Blood Donor' (dawca krwi, z podejrzeniem występowania zapalenia wątroby typu C) na 1
* '1=Hepatitis' (pacjent chory na jedynie zapalenie wątroby typu C) na 2
* '2=Fibrosis' (pacjent chory na zwłóknienie wątroby oraz zapalenie wątroby typu C) na 3
* '3=Cirrhosis' (pacjent chory na marskość wątroby oraz zapalenie wątroby typu C) na 4

b) 'Sex'
•	m (mężczyzna) na 0 
•	f (kobieta) na 1

Po konwersji z powrotem do ramki danych Sparka ramka wygląda następująco:


<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/df_2.JPG" />
</p>


Znów sprawdzam typy danych.


<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/typy_2.JPG" />
</p>
W kolejnym kroku zamieniam następnie kolumny ze stężeniami na float, wiek na int, podobnie jak w poprzednich krokach.</br>
Na koniec sprawdzam znów typy danych, by upewnić się, że są gotowe do wektoryzacji.


<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/typt_final.JPG" />
</p>

## Wektoryzacja

Wybieram wszystkie kolumny, a następnie korzystam z pakietu VectorAssembler.


<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/vectorizing.JPG" />
</p>

Do ramki danych dodaję kolumnę ze stworzonymi wektorami.


<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/vectors.JPG" />
</p>


## Zbudowanie modelu regresji logistycznej i drzewa decyzyjnego

Dzielę ramkę danych z wektorami na zbiór testowy i treningowy w proporcji 3:7.


<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/train.JPG" />
</p>


Importuję moduły regresji logistycznej i drzewa decyzyjnego z pakietu metod klasyfikacyjnych Sparka


<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/models.JPG" />
</p>


Przewidywane wartości wyglądają następująco:


<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/pred.JPG" />
</p>

## Ocena modelu

Do oceny modelu zmieniam typ danych w kolumnie "Category" na double, bo inaczej  moduł oceny klasyfikacji nie porówna różnych typów liczbowych

<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/casting.JPG" />
</p>

Za pomocą modułu MulticlassClassificationEvaluator porównuję kolumny ‘Category’ i  'prediction', a następnie obliczam parametry mojego modelu (wybieram te kolumny z  pred_y):

<p align="center">
<img src="https://github.com/TheLordWeirdSloughFeg/proj_pred_chor/blob/main/obrazki/Ocena%20modelu.JPG" />
</p>

#Wnioski
Dokładność wyniosła ok. 0.98, precyzja 1.00, recall ok. 0.99, a współczynnik F1 ok. 0.99. Model predykcji jest idealny do wykrywania wirusowego zapalenia wątroby typu C na podstawie zbadanych parametrów krwi, wieku oraz płci, ale może nie nadawać się do np. predykcji innych chorób wątroby. W oparciu o ten model i dane z wszystkich punktów firmy <b>Analit</b> można opracować kolejne modele przewidujące inne choroby wątroby, np. marskość lub zwłóknienie wątroby, pomagając szybciej diagnozować pacjentów.
