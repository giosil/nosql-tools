package org.dew.nosql.util;

public
class Obfuscator
{
  // [32 ( ) - 95 (_)]
  private final static String ENCRYPTION_KEY = "@X<:S=?'B;F)<=B>D@?=:D';@=B<?C;)@:'/=?A-X0=;(?1<X!";
  
  public static
  String encrypt(String sText)
  {
    if(sText == null) return null;
    
    int k = 0;
    StringBuilder sb = new StringBuilder(sText.length());
    for(int i = 0; i < sText.length(); i++) {
      if(k >= ENCRYPTION_KEY.length() - 1) {
        k = 0;
      }
      else {
        k++;
      }
      
      int c = sText.charAt(i);
      int d = ENCRYPTION_KEY.charAt(k);
      
      int r = c;
      if(c >= 32 && c <= 126) {
        r = r - d;
        if(r < 32) {
          r = 127 + r - 32;
        }
      }
      
      sb.append((char) r);
    }
    
    return sb.toString();
  }
  
  public static
  String decrypt(String sText)
  {
    if(sText == null) return null;
    
    int k = 0;
    StringBuilder sb = new StringBuilder(sText.length());
    for(int i = 0; i < sText.length(); i++) {
      if(k >= ENCRYPTION_KEY.length() - 1) {
        k = 0;
      }
      else {
        k++;
      }
      
      int c = sText.charAt(i);
      int d = ENCRYPTION_KEY.charAt(k);
      
      int r = c;
      if(c >= 32 && c <= 126) {
        r = r + d;
        if(r > 126) {
          r = 31 + r - 126;
        }
      }
      
      sb.append((char) r);
    }
    
    return sb.toString();
  }
}

