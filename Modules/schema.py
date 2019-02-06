#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''
Created on Sun Jan 27 19:24:49 2019

@author: xdzhuo
'''

from pyspark.sql.types import *

Schema = StructType([
    StructField('CSVId', IntegerType()),
    StructField('MachineIdentifier', StringType()),
    StructField('ProductName', StringType()),        
    StructField('EngineVersion', StringType()),
    StructField('AppVersion', StringType()),
    StructField('AvSigVersion', StringType()),
    StructField('IsBeta', DoubleType()),
    StructField('RtpStateBitfield', DoubleType()),       
    StructField('IsSxsPassiveMode', DoubleType()),
    StructField('DefaultBrowsersIdentifier', DoubleType()),
    StructField('AVProductStatesIdentifier', StringType()),
    StructField('AVProductsInstalled', DoubleType()),
    StructField('AVProductsEnabled', DoubleType()),
    StructField('HasTpm', DoubleType()),
    StructField('CountryIdentifier', StringType()),
    StructField('CityIdentifier', StringType()),                          
    StructField('OrganizationIdentifier', StringType()),
    StructField('GeoNameIdentifier', StringType()),
    StructField('LocaleEnglishNameIdentifier', StringType()),
    StructField('Platform', StringType()),
    StructField('Processor', StringType()),                           
    StructField('OsVer', StringType()),
    StructField('OsBuild', StringType()),
    StructField('OsSuite', StringType()), 
    StructField('OsPlatformSubRelease', StringType()),
    StructField('OsBuildLab', StringType()),
    StructField('SkuEdition', StringType()),
    StructField('IsProtected', DoubleType()),
    StructField('AutoSampleOptIn', ShortType()),
    StructField('PuaMode', StringType()),
    StructField('SMode', DoubleType()),        
    StructField('IeVerIdentifier', StringType()),
    StructField('SmartScreen', StringType()),
    StructField('Firewall', DoubleType()),
    StructField('UacLuaenable', DoubleType()),
    StructField('Census_MDC2FormFactor', StringType()),
    StructField('Census_DeviceFamily', StringType()),
    StructField('Census_OEMNameIdentifier', StringType()),
    StructField('Census_OEMModelIdentifier', StringType()),
    StructField('Census_ProcessorCoreCount', DoubleType()),
    StructField('Census_ProcessorManufacturerIdentifier', StringType()),
    StructField('Census_ProcessorModelIdentifier', StringType()),
    StructField('Census_ProcessorClass', DoubleType()),
    StructField('Census_PrimaryDiskTotalCapacity', DoubleType()),
    StructField('Census_PrimaryDiskTypeName', StringType()),
    StructField('Census_SystemVolumeTotalCapacity', DoubleType()),
    StructField('Census_HasOpticalDiskDrive', ShortType()),
    StructField('Census_TotalPhysicalRAM', DoubleType()),
    StructField('Census_ChassisTypeName', StringType()), 
    StructField('Census_InternalPrimaryDiagonalDisplaySizeInInches', DoubleType()),
    StructField('Census_InternalPrimaryDisplayResolutionHorizontal', DoubleType()),
    StructField('Census_InternalPrimaryDisplayResolutionVertical', DoubleType()),
    StructField('Census_PowerPlatformRoleName', StringType()),
    StructField('Census_InternalBatteryType', StringType()),
    StructField('Census_InternalBatteryNumberOfCharges', DoubleType()),
    StructField('Census_OSVersion', StringType()),
    StructField('Census_OSArchitecture', StringType()),
    StructField('Census_OSBranch', StringType()),
    StructField('Census_OSBuildNumber', StringType()),
    StructField('Census_OSBuildRevision', DoubleType()),
    StructField('Census_OSEdition', StringType()),
    StructField('Census_OSSkuName', StringType()),
    StructField('Census_OSInstallTypeName', StringType()),
    StructField('Census_OSInstallLanguageIdentifier', StringType()),
    StructField('Census_OSUILocaleIdentifier', StringType()),
    StructField('Census_OSWUAutoUpdateOptionsName', StringType()),
    StructField('Census_IsPortableOperatingSystem', DoubleType()),
    StructField('Census_GenuineStateName', StringType()),
    StructField('Census_ActivationChannel', StringType()),
    StructField('Census_IsFlightingInternal', DoubleType()),
    StructField('Census_IsFlightsDisabled', DoubleType()),
    StructField('Census_FlightRing', StringType()), 
    StructField('Census_ThresholdOptIn', DoubleType()),
    StructField('Census_FirmwareManufacturerIdentifier', StringType()),
    StructField('Census_FirmwareVersionIdentifier', StringType()),
    StructField('Census_IsSecureBootEnabled', DoubleType()),
    StructField('Census_IsWIMBootEnabled', DoubleType()),
    StructField('Census_IsVirtualDevice', DoubleType()),
    StructField('Census_IsTouchEnabled', DoubleType()),
    StructField('Census_IsPenCapable', DoubleType()),
    StructField('Census_IsAlwaysOnAlwaysConnectedCapable', DoubleType()),
    StructField('Wdft_IsGamer', DoubleType()),
    StructField('Wdft_RegionIdentifier', DoubleType()),
    StructField('HasDetections', DoubleType())
    ])

Cols = ['AVProductStatesIdentifier', 
        'AVProductsEnabled', 
        'AVProductsInstalled', 
        'AppVersion', 
        'AutoSampleOptIn', 
        'AvSigVersion', 
        'Census_ActivationChannel', 
        'Census_ChassisTypeName', 
        'Census_DeviceFamily', 
        'Census_FirmwareManufacturerIdentifier', 
        'Census_FirmwareVersionIdentifier', 
        'Census_FlightRing', 
        'Census_GenuineStateName', 
        'Census_HasOpticalDiskDrive', 
        'Census_InternalBatteryNumberOfCharges', 
        'Census_InternalBatteryType', 
        'Census_InternalPrimaryDiagonalDisplaySizeInInches', 
        'Census_InternalPrimaryDisplayResolutionHorizontal', 
        'Census_InternalPrimaryDisplayResolutionVertical', 
        'Census_IsAlwaysOnAlwaysConnectedCapable', 
        'Census_IsFlightingInternal', 
        'Census_IsFlightsDisabled', 
        'Census_IsPenCapable', 
        'Census_IsPortableOperatingSystem', 
        'Census_IsSecureBootEnabled', 
        'Census_IsTouchEnabled', 
        'Census_IsVirtualDevice', 
        'Census_IsWIMBootEnabled', 
        'Census_MDC2FormFactor', 
        'Census_OEMModelIdentifier', 
        'Census_OEMNameIdentifier', 
        'Census_OSArchitecture', 
        'Census_OSBranch', 
        'Census_OSBuildNumber', 
        'Census_OSBuildRevision', 
        'Census_OSEdition', 
        'Census_OSInstallLanguageIdentifier', 
        'Census_OSInstallTypeName', 
        'Census_OSSkuName', 
        'Census_OSUILocaleIdentifier', 
        'Census_OSVersion', 
        'Census_OSWUAutoUpdateOptionsName', 
        'Census_PowerPlatformRoleName', 
        'Census_PrimaryDiskTotalCapacity', 
        'Census_PrimaryDiskTypeName', 
        'Census_ProcessorClass', 
        'Census_ProcessorCoreCount', 
        'Census_ProcessorManufacturerIdentifier', 
        'Census_ProcessorModelIdentifier', 
        'Census_SystemVolumeTotalCapacity', 
        'Census_ThresholdOptIn', 
        'Census_TotalPhysicalRAM', 
        'CityIdentifier', 
        'CountryIdentifier', 
        'DefaultBrowsersIdentifier', 
        'EngineVersion', 
        'Firewall', 
        'GeoNameIdentifier', 
        'HasDetections', 
        'HasTpm', 
        'IeVerIdentifier', 
        'IsBeta', 
        'IsProtected', 
        'IsSxsPassiveMode', 
        'LocaleEnglishNameIdentifier', 
        'MachineIdentifier', 
        'OrganizationIdentifier', 
        'OsBuild', 
        'OsBuildLab', 
        'OsPlatformSubRelease', 
        'OsSuite', 
        'OsVer', 
        'Platform', 
        'Processor', 
        'ProductName', 
        'PuaMode', 
        'RtpStateBitfield', 
        'SMode', 
        'SkuEdition', 
        'SmartScreen', 
        'UacLuaenable', 
        'Wdft_IsGamer', 
        'Wdft_RegionIdentifier']

columns_struct_fields = [StructField('CSVId', IntegerType())]

for col in Cols:
    columns_struct_fields.append(Schema[col])
    
StreamSchema = StructType(columns_struct_fields)
    

