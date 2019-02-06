#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''
Created on Sun Jan 27 19:24:49 2019

@author: xdzhuo
'''

from pyspark.sql.types import *
# Defined on the true meaning of the data one by one

Schema = StructType([
    StructField('CSVId', IntegerType()),
    StructField('MachineIdentifier', StringType()),
    StructField('ProductName', StringType()),        
    StructField('EngineVersion', StringType()),
    StructField('AppVersion', StringType()),
    StructField('AvSigVersion', StringType()),
    StructField('IsBeta', ShortType()),
    StructField('RtpStateBitfield', ShortType()),       
    StructField('IsSxsPassiveMode', ShortType()),
    StructField('DefaultBrowsersIdentifier', DoubleType()),
    StructField('AVProductStatesIdentifier', StringType()),
    StructField('AVProductsInstalled', ShortType()),
    StructField('AVProductsEnabled', ShortType()),
    StructField('HasTpm', ShortType()),
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
    StructField('IsProtected', ShortType()),
    StructField('AutoSampleOptIn', ShortType()),
    StructField('PuaMode', ShortType()),
    StructField('SMode', ShortType()),        
    StructField('IeVerIdentifier', StringType()),
    StructField('SmartScreen', StringType()),
    StructField('Firewall', ShortType()),
    StructField('UacLuaenable', ShortType()),
    StructField('Census_MDC2FormFactor', StringType()),
    StructField('Census_DeviceFamily', StringType()),
    StructField('Census_OEMNameIdentifier', StringType()),
    StructField('Census_OEMModelIdentifier', StringType()),
    StructField('Census_ProcessorCoreCount', ShortType()),
    StructField('Census_ProcessorManufacturerIdentifier', StringType()),
    StructField('Census_ProcessorModelIdentifier', StringType()),
    StructField('Census_ProcessorClass', ShortType()),
    StructField('Census_PrimaryDiskTotalCapacity', IntegerType()),
    StructField('Census_PrimaryDiskTypeName', StringType()),
    StructField('Census_SystemVolumeTotalCapacity', IntegerType()),
    StructField('Census_HasOpticalDiskDrive', ShortType()),
    StructField('Census_TotalPhysicalRAM', Integer()),
    StructField('Census_ChassisTypeName', StringType()), 
    StructField('Census_InternalPrimaryDiagonalDisplaySizeInInches', DoubleType()),
    StructField('Census_InternalPrimaryDisplayResolutionHorizontal', IntegerType()),
    StructField('Census_InternalPrimaryDisplayResolutionVertical', IntegerType()),
    StructField('Census_PowerPlatformRoleName', StringType()),
    StructField('Census_InternalBatteryType', StringType()),
    StructField('Census_InternalBatteryNumberOfCharges', LongType()),
    StructField('Census_OSVersion', StringType()),
    StructField('Census_OSArchitecture', StringType()),
    StructField('Census_OSBranch', StringType()),
    StructField('Census_OSBuildNumber', StringType()),
    StructField('Census_OSBuildRevision', IntegerType()),
    StructField('Census_OSEdition', StringType()),
    StructField('Census_OSSkuName', StringType()),
    StructField('Census_OSInstallTypeName', StringType()),
    StructField('Census_OSInstallLanguageIdentifier', StringType()),
    StructField('Census_OSUILocaleIdentifier', StringType()),
    StructField('Census_OSWUAutoUpdateOptionsName', StringType()),
    StructField('Census_IsPortableOperatingSystem', ShortType()),
    StructField('Census_GenuineStateName', StringType()),
    StructField('Census_ActivationChannel', StringType()),
    StructField('Census_IsFlightingInternal', ShortType()),
    StructField('Census_IsFlightsDisabled', ShortType()),
    StructField('Census_FlightRing', StringType()), 
    StructField('Census_ThresholdOptIn', ShortType()),
    StructField('Census_FirmwareManufacturerIdentifier', StringType()),
    StructField('Census_FirmwareVersionIdentifier', StringType()),
    StructField('Census_IsSecureBootEnabled', ShortType()),
    StructField('Census_IsWIMBootEnabled', ShortType()),
    StructField('Census_IsVirtualDevice', ShortType()),
    StructField('Census_IsTouchEnabled', ShortType()),
    StructField('Census_IsPenCapable', ShortType()),
    StructField('Census_IsAlwaysOnAlwaysConnectedCapable', ShortType()),
    StructField('Wdft_IsGamer', ShortType()),
    StructField('Wdft_RegionIdentifier', ShortType()),
    StructField('HasDetections', ShortType())
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
    

