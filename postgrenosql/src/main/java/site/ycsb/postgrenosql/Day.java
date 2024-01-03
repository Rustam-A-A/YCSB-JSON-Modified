/*
 * Copyright 2017 YCSB Contributors. All Rights Reserved.
 *
 * CODE IS BASED ON the jdbc-binding JdbcDBClient class.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */





package site.ycsb.postgrenosql;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class Day {

  private String name;
  private String close;
  private String open;

  public Day(String name, String close, String open) {
    this.name = name;
    this.close = close;
    this.open = open;
  }

  public String getName() {
    return name;
  }

  public void setName(String name1) {
    this.name = name1;
  }

  public Day() {
  }

  public String getClose() {
    return close;
  }

  public void setClose(String close1) {
    this.close = close1;
  }

  public String getOpen() {
    return open;
  }

  public void setOpen(String open1) {
    this.open = open1;
  }
}
