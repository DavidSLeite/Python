class Carro:

    def __init__(self, marca: str, modelo: str, cor: str, ano: int, cambio_manual: int):
        self.marca = marca
        self.modelo = modelo
        self.cor = cor
        self.ano = ano
        self.tipo_cambio = "Manual" if cambio_manual == 1 else "Automático"
        self.velocidade = 0
        self.status_motor = 0

    def set_ligar(self):
        self.status_motor = True
        print("Carro ligado!")
        return self.status_motor

    def get_velocidade(self):
        if self.status_motor == True:
            print(f"A velocidade do carro atual é: {self.velocidade}")
        else:
            print("Não foi possível identificar a velocidade do carro, o carro está desligado.")
        return self.velocidade

    def set_desligar(self):
        self.status_motor = False
        print("Carro desligado!")
        return self.status_motor
    
    def set_acelerar(self):
        if self.status_motor == True:
            self.velocidade = self.velocidade + 1
            print("Aumentando a velocidade !!!")
        else:
            print("Não foi possível acelerar, o carro está desligado !!!")
        return self.velocidade

    def set_frear(self):
        if self.velocidade > 0:
            self.velocidade = self.velocidade - 1
            print("Reduzindo a velocidade !!!")
        return self.velocidade

# instanciando o carro
c = Carro("Volkswagem", "Gol", "Prata", 2017, 1)

# ligando o carro
c.set_ligar()

# pegando a velocidade atual
velocidade_atual = c.get_velocidade()

# acerelando o carro
for i in range(0, 5):
    c.set_acelerar()

velocidade_atual = c.get_velocidade()

# freando o carro
for i in range(0, 3):
    c.set_frear()

velocidade_atual = c.get_velocidade()

# em caso do carro desligado não é possível acelerar somente frear
c.set_desligar()

# acerelando o carro
for i in range(0, 180):
    c.set_acelerar()

velocidade_atual = c.get_velocidade()

# ligando o carro
c.set_ligar()

# freando o carro
for i in range(0, 1):
    c.set_frear()

velocidade_atual = c.get_velocidade()
