import { Component, inject, OnInit, signal } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { HeaderComponent } from "./layout/header/header.component";
import { HttpClient } from '@angular/common/http';
import { Product } from './shared/Models/product';
import { Pagination } from './shared/Models/pagination';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, HeaderComponent],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnInit {

  baseUrl = 'http://localhost:5130/api/';
  private httpClient = inject(HttpClient);
  title = "Footwears";
  products: Product[] = [];

  ngOnInit(): void {
      this.httpClient.get<Pagination<Product>>(this.baseUrl + 'products').subscribe({
        next: (response) => {
            console.log('Products fetched successfully:', response.data);
            this.products = response.data;
        },
        error: (error) => {
            console.error('Error fetching products:', error);
        },
        complete: () => {
            console.log('Product fetching completed.');
        }
      }
      );
  }

}
