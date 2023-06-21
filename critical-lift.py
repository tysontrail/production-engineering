import math

# Brill and Beggs correlation for gas compressibility factor

pressure = 689.5 # pressure, kPa

temperature = 148.9 # temperature, C

spec_gravity = 0.7 # gas specific gravity, unitless

mol_frac_n2 = 0.03 # mole fraction of nitrogen, unitless

mol_frac_co2 = 0.06 # mole fraction of carbon dioxide, unitless

mol_frac_h2s = 0.04 # mole fraction of hydrogen sulfide, unitless

pseudo_crit_press = (678-50*(spec_gravity-0.5)-206.7*mol_frac_n2+440*mol_frac_co2+606.7*mol_frac_h2s)*6.894757 # pseudo-critical pressure, kPa
print()
print('Pseudo-critical pressure =', round(pseudo_crit_press,4), 'kPa')

pseudo_crit_temp = (326+315.7*(spec_gravity-0.5)-240*mol_frac_n2-83.3*mol_frac_co2+133.3*mol_frac_h2s)*5/9 # pseudo-critical temperature, K

print('Pseudo-critical temperature =', round(pseudo_crit_temp,4), 'K')

pseudo_red_press = pressure/pseudo_crit_press # pseudo-reduced pressure, unitless

print('Pseudo-reduced pressure =', round(pseudo_red_press,4))

pseudo_red_temp = (temperature+273.15)/pseudo_crit_temp # pseudo-reduced temperature, unitless

print('Pseudo-reduced temperature =', round(pseudo_red_temp,4))

A = 1.39*(pseudo_red_temp-0.92)**0.5-0.36*pseudo_red_temp-0.101 # A parameter, unitless

print('A =', round(A,4))

B = (0.62-0.23*pseudo_red_temp)*pseudo_red_press+(0.066/(pseudo_red_temp-0.86)-0.037)*pseudo_red_press**2+0.32*pseudo_red_press**6/(10**A) # B parameter, unitless

print('B =', round(B,4))

C = 0.132-0.32*math.log10(pseudo_red_temp) # C parameter, unitless

print('C =', round(C,4))

D = 10**(0.3106-0.49*pseudo_red_temp+0.1824*pseudo_red_temp**2) # D parameter, unitless

print('D =', round(D,4))

Z = A+(1-A)/math.exp(B)+C*pseudo_red_press**D # compressibility factor, unitless

print('Compressibility factor =', round(Z,4))

print()