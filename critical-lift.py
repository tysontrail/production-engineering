import math

def gas_compressibility_factor(pressure,temperature, spec_gravity, mol_frac_n2, mol_frac_co2, mol_frac_h2s):
    """
        Calculates the gas compressibility factor (Z) using the Brill and Beggs correlation.

        Args:
            pressure (float): Instantaneous pressure in units of pressure.
            temperature (float): Instantaneous temperature in units of temperature.
            spec_gravity (float): Specific gravity of the gas.
            mol_frac_n2 (float): Molar fraction of nitrogen (N2) in the gas.
            mol_frac_co2 (float): Molar fraction of carbon dioxide (CO2) in the gas.
            mol_frac_h2s (float): Molar fraction of hydrogen sulfide (H2S) in the gas.

        Returns:
            float: Gas compressibility factor (Z), unitless.
        """
    pseudo_crit_press = (678-50*(spec_gravity-0.5)-206.7*mol_frac_n2+440*mol_frac_co2+606.7*mol_frac_h2s)*6.894757 # pseudo-critical pressure, kPa

    pseudo_crit_temp = (326+315.7*(spec_gravity-0.5)-240*mol_frac_n2-83.3*mol_frac_co2+133.3*mol_frac_h2s)*5/9 # pseudo-critical temperature, K

    pseudo_red_press = pressure/pseudo_crit_press # pseudo-reduced pressure, unitless

    pseudo_red_temp = (temperature+273.15)/pseudo_crit_temp # pseudo-reduced temperature, unitless

    A = 1.39*(pseudo_red_temp-0.92)**0.5-0.36*pseudo_red_temp-0.101 # A parameter, unitless

    B = (0.62-0.23*pseudo_red_temp)*pseudo_red_press+(0.066/(pseudo_red_temp-0.86)-0.037)*pseudo_red_press**2+0.32*pseudo_red_press**6/(10**A) # B parameter, unitless

    C = 0.132-0.32*math.log10(pseudo_red_temp) # C parameter, unitless

    D = 10**(0.3106-0.49*pseudo_red_temp+0.1824*pseudo_red_temp**2) # D parameter, unitless

    Z = A+(1-A)/math.exp(B)+C*pseudo_red_press**D # compressibility factor, unitless

    return Z

print(gas_compressibility_factor(1000, 100, 0.6, 0.01, 0.01, 0.01))