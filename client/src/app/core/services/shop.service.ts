import { HttpClient, HttpParams } from '@angular/common/http';
import { inject, Injectable, Service } from '@angular/core';
import { Pagination } from '../../shared/Models/pagination';
import { Product } from '../../shared/Models/product';
import { ShortParams } from '../../shared/Models/shortParams';

@Injectable({
  providedIn: 'root'
})
export class ShopService {
      baseUrl = 'http://localhost:5130/api/';
      private httpClient = inject(HttpClient);
      public brandsList: string[] = [];
      public typesList: string[] = [];


      getProducts(shopParams: ShortParams) {
            let params = new HttpParams();
            
            if (shopParams.brands && shopParams.brands.length > 0) {
                  params = params.append('brands', shopParams.brands.join(','));
            }
            if (shopParams.types && shopParams.types.length > 0) {
                  params = params.append('types', shopParams.types.join(','));
            }
            if (shopParams.sort) {
                  params = params.append('Sort', shopParams.sort);
            }

            if (shopParams.pageNumber) {
                  params = params.append('pageIndex', shopParams.pageNumber.toString());
            }
            if (shopParams.pageSize) {
                  params = params.append('pageSize', shopParams.pageSize.toString());
            }

            if (shopParams.search) {
                  params = params.append('search', shopParams.search);
            }

            return this.httpClient.get<Pagination<Product>>(this.baseUrl + 'products', { params });
      }

      getProductById(id: number) {
            return this.httpClient.get<Product>(this.baseUrl + 'products/' + id);
      }

      getBrands() {
            if (this.brandsList.length > 0) {
                  return;
            }
            return this.httpClient.get<string[]>(this.baseUrl + 'products/brands').subscribe({
                  next: (response) => {
                        this.brandsList = response;
                  },
                  error: (error) => {
                        console.error('Error fetching brands:', error);
                  }
            });
      }

      getTypes() {
            if (this.typesList.length > 0) {
                  return;
            }
            return this.httpClient.get<string[]>(this.baseUrl + 'products/types').subscribe({
                  next: (response) => {
                        this.typesList = response;
                  },
                  error: (error) => {
                        console.error('Error fetching types:', error);
                  }
            });
      }
}
